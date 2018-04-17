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
package com.gridsum.impala.util;

public class ImpalaConstants {

    public static final String GS_ANNOTATION = "--gs-annotation ";

    /**
     * gs annotation key
     */
    public static final String RETRY_KEY = "retry";
    public static final String ERROR_KEY = "error";

    /**
     * back up key
     */
    public static final String BACK_UP_KEY_HOST = "host";
    public static final String BACK_UP_KEY_KRBHOSTFQDN = "KrbHostFQDN";
    public static final String BACK_UP_KEY_ERROR_TIME = "errorTime";

    public static final String TTRANSPORT_EXCEPTION = "org.apache.thrift.transport.TTransportException";
    public static final String CATALOG_UPDATE_EXCEPTION = "Waiting for catalog update from the StateStore";

    /**
     * Delegation Key
     */
    public static final String IMPALA_DELEGATION_KEY = "impala.doas.user";
    public static final String HIVE_DELEGATION_KEY = "hive.server2.proxy.user";


    /**
     * Set Impala Memory Limit
     */
    public final static String MEM_LIMIT = "SET MEM_LIMIT = %s";

}
