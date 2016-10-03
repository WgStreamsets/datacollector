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

import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.IgnoreTableFilter;
import com.streamsets.pipeline.stage.origin.mysql.schema.Column;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FiltersTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnInvalidFormat() {
        IgnoreTableFilter filter = new IgnoreTableFilter("T1");
        filter.apply(event("a", "t"));
    }

    @Test
    public void shouldFilterOutByDbAndTableName() {
        IgnoreTableFilter filter = new IgnoreTableFilter("A.T1");
        assertThat(filter.apply(event("a", "t")), is(Filter.Result.PASS));
        assertThat(filter.apply(event("a", "T1")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("A", "t1")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("B", "T1")), is(Filter.Result.PASS));
        assertThat(filter.apply(event(" a", " t1 ")), is(Filter.Result.DISCARD));
    }

    @Test
    public void shouldFilterOutByTableNames() {
        Filter filter = new IgnoreTableFilter("A.T1").and(
                new IgnoreTableFilter("B.T2")
        );
        assertThat(filter.apply(event("a", "t")), is(Filter.Result.PASS));
        assertThat(filter.apply(event("a", "T1")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("a", "T2")), is(Filter.Result.PASS));
        assertThat(filter.apply(event("B", "t2")), is(Filter.Result.DISCARD));
    }


    @Test
    public void shouldFilterOutByTableNameWithWildcards() {
        Filter filter = new IgnoreTableFilter("A%.T%1");
        assertThat(filter.apply(event("a", "t")), is(Filter.Result.PASS));
        assertThat(filter.apply(event("a", "t12")), is(Filter.Result.PASS));
        assertThat(filter.apply(event("a", "T1")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("a", "Ta1")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("a", "Ta2221")), is(Filter.Result.DISCARD));
        assertThat(filter.apply(event("ab", "Ta2221")), is(Filter.Result.DISCARD));
    }

    private EnrichedEvent event(String db, String tableName) {
        Table table = new Table(db, tableName, Collections.<Column>emptyList());
        return new EnrichedEvent(null, table, null);
    }
}
