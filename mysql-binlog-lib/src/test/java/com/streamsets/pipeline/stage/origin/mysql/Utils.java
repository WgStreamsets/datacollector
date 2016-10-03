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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static void runInitScript(String initScriptPath, DataSource dataSource) throws SQLException {
        try(Connection conn = dataSource.getConnection()) {
            try {
                URL resource = Resources.getResource(initScriptPath);
                String sql = Resources.toString(resource, Charsets.UTF_8);
                ScriptUtils.executeSqlScript(conn, initScriptPath, sql);
                conn.commit();
                conn.close();
            } catch (IOException | IllegalArgumentException e) {
                LOGGER.warn("Could not load classpath init script: {}", initScriptPath);
                throw new SQLException("Could not load classpath init script: " + initScriptPath, e);
            } catch (ScriptException e) {
                LOGGER.error("Error while executing init script: {}", initScriptPath, e);
                throw new SQLException("Error while executing init script: " + initScriptPath, e);
            }
        }
    }
}
