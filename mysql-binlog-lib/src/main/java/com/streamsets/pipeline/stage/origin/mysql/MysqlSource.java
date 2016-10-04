/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.stage.origin.mysql.filters.IncludeTableFilter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.PoolInitializationException;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filters;
import com.streamsets.pipeline.stage.origin.mysql.filters.IgnoreTableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class MysqlSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

    private BinaryLogConsumer consumer;

    private BinaryLogClient client;

    private EventBuffer eventBuffer;

    private HikariDataSource dataSource;

    private SourceOffsetFactory offsetFactory;

    private Filter filter;

    private final BlockingQueue<ServerException> serverErrors = new LinkedBlockingQueue<>();

    private final RecordConverter recordConverter = new RecordConverter(new RecordFactory() {
        @Override
        public Record create(String recordSourceId) {
            return getContext().createRecord(recordSourceId);
        }
    });

    public abstract MysqlSourceConfig getConfig();

    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();

        // check if binlog client connection is possible
        BinaryLogClient client = createBinaryLogClient();
        client.setServerId(getConfig().serverId);

        try {
            client.setKeepAlive(false);
            client.connect(getConfig().connectTimeout);
        } catch (IOException | TimeoutException e) {
            LOG.error("Error connecting to MySql binlog: {}", e.getMessage(), e);
            issues.add(getContext().createConfigIssue(
                    Groups.MYSQL.name(), null, Errors.MYSQL_003, e.getMessage(), e
            ));
        } finally {
            try {
                client.disconnect();
            } catch (IOException e) {
                LOG.warn("Error disconnecting from MySql: {}", e.getMessage(), e);
            }
        }

        // create include/ignore filters
        Filter includeFilter = null;
        try {
            includeFilter = createIncludeFilter();
        } catch (IllegalArgumentException e) {
            LOG.error("Error creating include tables filter: {}", e.getMessage(), e);
            issues.add(getContext().createConfigIssue(
                    Groups.MYSQL.name(), "Include tables", Errors.MYSQL_008, e.getMessage(), e
            ));
        }

        try {
            Filter ignoreFilter = createIgnoreFilter();
            filter = includeFilter.and(ignoreFilter);
        } catch (IllegalArgumentException e) {
            LOG.error("Error creating ignore tables filter: {}", e.getMessage(), e);
            issues.add(getContext().createConfigIssue(
                    Groups.MYSQL.name(), "Ignore tables", Errors.MYSQL_007, e.getMessage(), e
            ));
        }

        // connect to mysql
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%d", getConfig().hostname, getConfig().port));
        hikariConfig.setUsername(getConfig().username);
        hikariConfig.setPassword(getConfig().password);
        hikariConfig.setReadOnly(true);
        hikariConfig.addDataSourceProperty("useSSL", false); // TODO make configurable
        try {
            dataSource = new HikariDataSource(hikariConfig);
            offsetFactory = isGtidEnabled() ? SourceOffsetFactory.GTID : SourceOffsetFactory.BIN_LOG;
        } catch (PoolInitializationException e) {
            LOG.error("Error connecting to MySql: {}", e.getMessage(), e);
            issues.add(getContext().createConfigIssue(
                    Groups.MYSQL.name(), null, Errors.MYSQL_003, e.getMessage(), e
            ));
        }
        return issues;
    }

    private BinaryLogClient createBinaryLogClient() {
        return new BinaryLogClient(getConfig().hostname, getConfig().port, getConfig().username, getConfig().password);
    }

    @Override
    public void destroy() {
        if (client != null && client.isConnected()) {
            try {
                client.disconnect();
            } catch (IOException e) {
                LOG.warn("Error disconnecting from MySql", e);
            }
        }

        if (dataSource != null) {
            dataSource.close();
        }

        super.destroy();
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        // client does not get connected in init(), instead it is connected on first
        // invocation of this produce(), as we need to advance it to specific offset
        if (client == null) {
            // connect consumer handling all events to client
            MysqlSchemaRepository schemaRepository = new MysqlSchemaRepository(dataSource);
            eventBuffer = new EventBuffer(getConfig().maxBatchSize);
            client = createBinaryLogClient();
            consumer = new BinaryLogConsumer(schemaRepository, eventBuffer, client, filter);

            connectClient(client, lastSourceOffset);
            LOG.info("Connected client with configuration: {}", getConfig());
        }

        // in case of empty batch we don't want to stop
        if (lastSourceOffset == null) {
            lastSourceOffset = "";
        }

        // since last invocation there could errors happen
        handleErrors();

        int recordCounter = 0;
        int batchSize = getConfig().maxBatchSize > maxBatchSize ? maxBatchSize : getConfig().maxBatchSize;
        long startTime = System.currentTimeMillis();
        while (recordCounter < batchSize && (startTime + getConfig().maxWaitTime) > System.currentTimeMillis()) {
            long timeLeft = getConfig().maxWaitTime - (System.currentTimeMillis() - startTime);
            if (timeLeft < 0) {
                break;
            }
            EnrichedEvent event = eventBuffer.poll(timeLeft, TimeUnit.MILLISECONDS);
            // check errors
            handleErrors();

            if (event != null) {
                List<Record> records = recordConverter.toRecords(event);
                // If we are in preview mode, make sure we don't send a huge number of messages.
                if (getContext().isPreview() && recordCounter + records.size() > batchSize) {
                    records = records.subList(0, batchSize - recordCounter);
                }
                for (Record record : records) {
                    batchMaker.addRecord(record);
                }
                recordCounter += records.size();
                lastSourceOffset = event.getOffset().format();
            }
        }
        return lastSourceOffset;
    }

    private void connectClient(BinaryLogClient client, String lastSourceOffset) throws StageException {
        try {
            if (lastSourceOffset == null) {
                // first start
                if (getConfig().initialOffset != null && !getConfig().initialOffset.isEmpty()) {
                    // start from config offset
                    SourceOffset offset = offsetFactory.create(getConfig().initialOffset);
                    LOG.info("Moving client to offset {}", offset);
                    offset.positionClient(client);
                    consumer.setOffset(offset);
                } else if (getConfig().startFromBeginning) {
                    if (isGtidEnabled()) {
                        // when starting from beginning with GTID - skip GTIDs that have been removed from server logs already
                        GtidSet purged = new GtidSet(Util.getServerGtidPurged(dataSource));
                        // client's gtidset includes first event of purged, skip latest tx of purged
                        for (GtidSet.UUIDSet uuidSet : purged.getUUIDSets()) {
                            GtidSet.Interval last = null;
                            for (GtidSet.Interval interval : uuidSet.getIntervals()) {
                                last = interval;
                            }
                            purged.add(uuidSet.getUUID().toString() + ":" + String.valueOf(last.getEnd()));
                        }
                        LOG.info("Skipping purged gtidset {}", purged);
                        client.setGtidSet(purged.toString());
                    } else {
                        // gtid_mode = off
                        client.setBinlogFilename("");
                    }
                } else {
                    // read from current position
                    if (isGtidEnabled()) {
                        // set client gtidset to master executed gtidset
                        String executed = Util.getServerGtidExecuted(dataSource);
                        // if position client to 'executed' - it will fetch last transaction
                        // so - advance client to +1 transaction
                        String serverUUID = Util.getGlobalVariable(dataSource, "server_uuid");
                        GtidSet ex = new GtidSet(executed);
                        for (GtidSet.UUIDSet uuidSet : ex.getUUIDSets()) {
                            if (uuidSet.getUUID().equals(serverUUID)) {
                                List<GtidSet.Interval> intervals = new ArrayList<>(uuidSet.getIntervals());
                                GtidSet.Interval last = intervals.get(intervals.size() - 1);
                                ex.add(String.format("%s:%d", serverUUID, last.getEnd()));
                                break;
                            }
                        }
                        client.setGtidSet(ex.toString());
                    } else {
                        // do nothing, in case of binlog replication client positions at current position by default
                    }
                }
            } else {
                // resume work from previous position
                if (!"".equals(lastSourceOffset)) {
                    SourceOffset offset = offsetFactory.create(lastSourceOffset);
                    LOG.info("Moving client to offset {}", offset);
                    offset.positionClient(client);
                    consumer.setOffset(offset);
                }
            }

            client.setKeepAlive(true);
            client.setKeepAliveInterval(getConfig().connectTimeout);
            registerClientLifecycleListener();
            client.registerEventListener(consumer);
            client.connect(getConfig().connectTimeout);
        } catch (IOException | TimeoutException | SQLException e) {
            LOG.error(Errors.MYSQL_003.getMessage(), e.toString(), e);
            throw new StageException(Errors.MYSQL_003, e.toString(), e);
        }
    }

    private Filter createIgnoreFilter() {
        Filter filter = Filters.PASS;
        if (getConfig().ignoreTables != null) {
            for (String table : getConfig().ignoreTables.split(",")) {
                if (!table.isEmpty()) {
                    filter = filter.and(new IgnoreTableFilter(table));
                }
            }
        }
        return filter;
    }

    private Filter createIncludeFilter() {
        // if there are no include filters - pass
        Filter filter = Filters.PASS;
        if (getConfig().includeTables != null) {
            String[] includeTables = getConfig().includeTables.split(",");
            if (includeTables.length > 0) {
                // ignore all that is not explicitly included
                filter = Filters.DISCARD;
                for (String table : includeTables) {
                    if (!table.isEmpty()) {
                        filter = filter.or(new IncludeTableFilter(table));
                    }
                }
            }
        }
        return filter;
    }

    private void registerClientLifecycleListener() {
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                if (ex instanceof ServerException) {
                    serverErrors.add((ServerException) ex);
                } else {
                    LOG.error("Unhandled communication error: {}", ex.getMessage(), ex);
                }
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                LOG.error("Error deserializing event: {}", ex.getMessage(), ex);
            }
        });
    }

    private boolean isGtidEnabled() {
        try {
            return "ON".equals(Util.getGlobalVariable(dataSource, "gtid_mode"));
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private void handleErrors() throws StageException {
        // handle server errors
        for (ServerException e : serverErrors) {
            // record policy does not matter - stop pipeline
            throw new StageException(Errors.MYSQL_006, e.getMessage(), e);
        }

        // handle records errors
        for (EventError e : eventBuffer.resetErrors()) {
            switch (getContext().getOnErrorRecord()) {
                case DISCARD:
                    break;
                case TO_ERROR:
                    getContext().reportError(
                            Errors.MYSQL_004,
                            e.getEvent(),
                            e.getOffset(),
                            e.getException().toString(),
                            e.getException()
                    );
                    break;
                case STOP_PIPELINE:
                    if (e.getException() instanceof StageException) {
                        throw (StageException) e.getException();
                    } else {
                        throw new StageException(
                                Errors.MYSQL_004,
                                e.getEvent(),
                                e.getOffset(),
                                e.getException().toString(),
                                e.getException()
                        );
                    }
                default:
                    throw new IllegalStateException(String.format("Unknown On Error Value '%s'",
                            getContext().getOnErrorRecord()), e.getException());
            }
        }
    }
}
