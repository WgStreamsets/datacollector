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

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "JDBC Producer",
    description = "Writes data to a JDBC destination.",
    icon = "rdbms.png")
@ConfigGroups(value = Groups.class)
public class JdbcDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "JDBC"
  )
  public String connectionString;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 130,
      group = "JDBC"
  )
  public Map<String, String> driverProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      defaultValue = "false",
      displayPosition = 25,
      group = "JDBC"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      defaultValue = "",
      displayPosition = 10,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      defaultValue = "",
      displayPosition = 20,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Fully Qualified Table Name",
      description = "Table write to, e.g. <schema>.<table_name>",
      displayPosition = 30,
      group = "JDBC"
  )
  public String qualifiedTableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Field to Column Mapping",
      description = "Fields to map to RDBMS columns. To avoid errors, field data types must match.",
      displayPosition = 40,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldMappingConfig> columnNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Rollback Batch on Error",
      description = "Whether or not to rollback the entire batch on error. Some JDBC drivers provide information" +
          "about individual failed rows, and can insert partial batches.",
      displayPosition = 50,
      group = "JDBC"
  )
  public boolean rollbackOnError;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "JDBC Driver Class Name",
      description = "Class name for pre-JDBC 4 compliant drivers.",
      displayPosition = 140,
      group = "LEGACY"
  )
  public String driverClassName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Connection Health Test Query",
      description = "Not recommended for JDBC 4 compliant drivers. Runs when a new database connection is established.",
      displayPosition = 150,
      group = "LEGACY"
  )
  public String connectionTestQuery;

  @Override
  protected Target createTarget() {
    return new JdbcTarget(connectionString, username, password, qualifiedTableName, columnNames,
        rollbackOnError, driverProperties, driverClassName, connectionTestQuery);
  }
}