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

import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-memory buffer for events collected from {@link com.github.shyiko.mysql.binlog.BinaryLogClient} and
 * possible errors happened during collection.
 * <p/>
 * Events are enriched with corresponding table metadata and offset.
 */
public class EventBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

    private final ArrayBlockingQueue<EnrichedEvent> queue;

    public EventBuffer(int batchSize) {
        this.queue = new ArrayBlockingQueue<>(batchSize);
    }

    private final Vector<EventError> errors = new Vector<>();

    /**
     * Read next event from buffer with respect to maximum timeout.
     * @param timeout timeout.
     * @param unit timeout time unit.
     * @return next event of null
     * @throws StageException
     */
    public EnrichedEvent poll(long timeout, TimeUnit unit) throws StageException {
        try {
            EnrichedEvent event = queue.poll(timeout, unit);
            return event;
        } catch (InterruptedException e) {
            LOG.error(Errors.MYSQL_001.getMessage(), e.toString(), e);
            Thread.currentThread().interrupt();
            throw new StageException(Errors.MYSQL_001, e.toString(), e);
        }
    }

    public boolean put(EnrichedEvent event) {
        try {
            queue.put(event);
            return true;
        } catch (InterruptedException e) {
            LOG.error("Error adding event to buffer, reason: {}", e.toString(), e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Add error to errors list.
     * @param e
     */
    public void putError(EventError e) {
        errors.add(e);
    }

    /**
     * Get errors if any and clear errors.
     * @return errors if any exist.
     */
    public List<EventError> resetErrors() {
        synchronized (errors) {
            List<EventError> res = new ArrayList<>(errors);
            errors.clear();
            return res;
        }
    }
}
