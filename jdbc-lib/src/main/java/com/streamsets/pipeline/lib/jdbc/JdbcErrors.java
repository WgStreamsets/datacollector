/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum JdbcErrors implements ErrorCode {

  JDBC_00("Cannot connect to specified database: {}"),
  JDBC_01("Failed to evaluate expression: '{}'"),
  JDBC_02("Exception executing query: '{}' - '{}'"),
  JDBC_03("Failed to parse column '{}' to field with value {}."),
  JDBC_04("No results for query: '{}'"),
  JDBC_05("Unsupported data type in record: {}"),
  JDBC_06("Failed to initialize connection pool: {}"),
  JDBC_07("Invalid column mapping from field '{}' to column '{}'"),
  JDBC_08("Record missing required field {} for change log type {}"),
  JDBC_09("Invalid operation '{}' for change log type {}"),
  JDBC_10("'{}' is less than the minimum value of '{}'"),
  JDBC_11("Minimum Idle Connections ({}) must be less than or equal to Maximum Pool Size ({})"),
  JDBC_13("Failed to convert CLOB to string: {}"),
  JDBC_14("Error processing batch.\n{}"),
  JDBC_15("Invalid JDBC Namespace prefix, should end with '.'"),
  JDBC_16("Table '{}' does not exist or PDB is incorrect. Make sure the correct PDB was specified"),
  JDBC_17("Failed to lookup primary keys for table '{}' : {}"),
  JDBC_19("Record did not contain primary key field mapped to primary key column '{}'"),
  JDBC_20("Could not parse the table name template expression: {}"),
  JDBC_21("Could not evaluate the table name template expression: {}"),
  JDBC_22("The record had no fields that matched the columns in the destination table."),
  JDBC_23("The field '{}' of type '{}' doesn't match the destination column's type."),
  JDBC_24("No results from insert"),
  JDBC_25("No column mapping for column '{}'"),
  JDBC_26("Invalid table name template expression '{}': {}"),
  JDBC_27("The query interval expression must be greater than or equal to zero."),
  JDBC_28("Failed to create JDBC driver, JDBC driver JAR may be missing: {}"),
  JDBC_29("Query must include '{}' in WHERE clause and in ORDER BY clause before other columns."),
  JDBC_30("The JDBC driver for this database does not support scrollable cursors, " +
      "which are required when Transaction ID Column Name is specified."),
  JDBC_31("Query result has duplicate column label '{}'. Create an alias using 'AS' in your query."),
  JDBC_32("Offset Column '{}' cannot contain a '.' or prefix."),
  JDBC_33("Offset column '{}' not found in query '{}' results."),
  JDBC_34("Query failed to execute: '{}' Error: {}"),
  JDBC_35("Parsed record had {} columns but SDC expected {}."),
  JDBC_36("Column index {} is not valid."),
  JDBC_37("Unsupported type {} for column {}"),
  JDBC_38("Query must include '{}' clause."),
  JDBC_39("Oracle SID must be specified for Oracle 12c"),
  JDBC_40("Error while switching to container {} using given credentials"),
  JDBC_41("Error while getting DB version"),
  JDBC_42("Error while getting initial SCN. Please verify the privileges for the user"),
  JDBC_43("Could not parse redo log statement: {}"),
  JDBC_44("Error while getting changes from Oracle due to error: {}"),
  JDBC_45("Redo logs are not available for the specified start date. " +
      "Provide a more recent start date"),
  JDBC_46("Redo logs are not available for the specified initial SCN. " +
      "Provide a more recent initial SCN"),
  JDBC_47("The current latest SCN {} is less than the initial SCN"),
  JDBC_48("The start date is in the future"),
  JDBC_49("Date is invalid. Please use format DD-MM-YYYY HH24:MM:SS"),
  JDBC_50("Error while getting table schema. Please verify the connectivity to the DB and the privileges for the user"),
  JDBC_51("Invalid value: {}"),
  JDBC_52("Error starting LogMiner"),
  ;

  private final String msg;

  JdbcErrors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
