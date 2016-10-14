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

public class BinLogPositionSourceOffset implements SourceOffset {
    private final long position;
    private final String filename;

    public BinLogPositionSourceOffset(String filename, long position) {
        this.filename = filename;
        this.position = position;
    }

    @Override
    public String format() {
        return String.format("%s:%s", filename, position);
    }

    @Override
    public void positionClient(BinaryLogClient client) {
        client.setBinlogFilename(filename);
        client.setBinlogPosition(position);
    }

    public long getPosition() {
        return position;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinLogPositionSourceOffset that = (BinLogPositionSourceOffset) o;

        if (position != that.position) return false;
        return filename != null ? filename.equals(that.filename) : that.filename == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (position ^ (position >>> 32));
        result = 31 * result + (filename != null ? filename.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BinLogPositionSourceOffset{" + format() + "}";
    }

    public static BinLogPositionSourceOffset parse(String offset) {
        String[] a = offset.split(":");
        String filename = a[0];
        long position = Long.valueOf(a[1]);
        return new BinLogPositionSourceOffset(filename, position);
    }
}
