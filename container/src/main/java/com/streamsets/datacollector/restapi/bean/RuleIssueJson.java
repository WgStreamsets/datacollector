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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.datacollector.util.NullDeserializer;

import java.util.Map;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class RuleIssueJson {

  private final com.streamsets.datacollector.validation.RuleIssue ruleIssue;

  public RuleIssueJson(com.streamsets.datacollector.validation.RuleIssue ruleIssue) {
    this.ruleIssue = ruleIssue;
  }

  public Map getAdditionalInfo() {
    return ruleIssue.getAdditionalInfo();
  }

  public String getMessage() {
    return ruleIssue.getMessage();
  }

  public String getRuleId() {
    return ruleIssue.getRuleId();
  }

  @JsonIgnore
  public com.streamsets.datacollector.validation.RuleIssue getRuleIssue() {
    return ruleIssue;
  }
}