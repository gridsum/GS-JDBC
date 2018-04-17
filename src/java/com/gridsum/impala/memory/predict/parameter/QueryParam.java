/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gridsum.impala.memory.predict.parameter;

/**
 * Query Parameter for Memory Predict Service
 */
public class QueryParam {

    /**
     * Sql need predict memory
     */
    private String sql;
    /**
     * Database that sql use
     */
    private String db;
    /**
     * User for memory predict service
     * it help memory predict service chose the right model.
     * Default value is 'default' means use the common model
     */
    private String pool;

    public QueryParam() {
    }

    public String getSql() {
        return sql;
    }

    public QueryParam setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public String getDb() {
        return db;
    }

    public QueryParam setDb(String db) {
        this.db = db;
        return this;
    }

    public String getPool() {
        return pool;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }
}
