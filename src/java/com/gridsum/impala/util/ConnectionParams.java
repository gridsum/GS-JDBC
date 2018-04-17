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

import java.util.ArrayList;


public class ConnectionParams {
    /**
     * delegation parameter
     */
    public static final String DELEGATION = "DelegationUID";
    /**
     * back_up parameter
     */
    public static final String BACK_UP = "BACK_UP";
    /**
     * database parameter
     */
    public static final String DB = "DB";


    /**
     * request_pool parameter
     */
    public static final String IMPALA_REQUEST_POOL = "REQUEST_POOL";

    /**
     * mem_limit parameter
     */

    public static final String IMPALA_MEL_LIMIT = "MEM_LIMIT";

    /**
     * memory predict service parameter
     */
    public static final String IMPALA_PREDICT_MEM_AUTO = "PREDICT_MEM_AUTO";

    /**
     * when sql retry execute , this num limit the max memory
     */

    public static final String IMPALA_MAX_RETRY_MEM = "MAX_RETRY_MEM";

    /**
     * Cloudera Manager API parameter
     */
    public static final String CM_API_URL = "CM_API_URL";
    public static final String CM_API_USERNAME = "CM_API_USERNAME";
    public static final String CM_API_PASSWORD = "CM_API_PASSWORD";
    public static final String CM_API_CLUSTER_NAME = "CM_API_CLUSTER_NAME";
    public static final String CM_API_IMPALA_SERVICE_NAME = "CM_API_IMPALA_SERVICE_NAME";
    public static final String IMPALA_QUERY_PORT = "IMPALA_QUERY_PORT";

    /**
     * max retry count
     */
    public static final String IMPALA_RETRY_COUNT = "RETRY_COUNT";

    /**
     * all parameter that can be used in 'set ' mode
     */
    public static final java.util.List<String> PARAMETERS = new ArrayList<String>();

    static {
        //both impala and hive
        PARAMETERS.add(BACK_UP);
        //impala only
        PARAMETERS.add(IMPALA_MEL_LIMIT);
        PARAMETERS.add(IMPALA_PREDICT_MEM_AUTO);
        PARAMETERS.add(IMPALA_MAX_RETRY_MEM);
        PARAMETERS.add(CM_API_URL);
        PARAMETERS.add(CM_API_USERNAME);
        PARAMETERS.add(CM_API_PASSWORD);
        PARAMETERS.add(CM_API_CLUSTER_NAME);
        PARAMETERS.add(CM_API_IMPALA_SERVICE_NAME);
        PARAMETERS.add(IMPALA_QUERY_PORT);
        PARAMETERS.add(IMPALA_RETRY_COUNT);
    }

}

