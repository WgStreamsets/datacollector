/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  // Configuration errors
  JDBCDEST_00("Cannot connect to specified database: {}"),
  JDBCDEST_01("Failed to create JDBC driver, JDBC driver JAR may be missing: {}"),
  JDBCDEST_02("Failed to insert record: '{}' Error: {}"),
  JDBCDEST_03("Failed to initialize connection pool: {}"),
  JDBCDEST_04("Invalid column mapping: {}"),
  JDBCDEST_05("Unsupported data type in record: {}"),
  JDBCDEST_06("Failed to prepare insert statement: {}"),
  JDBCDEST_07("JDBC connection cleanup failed: {}")
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}