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

package com.gridsum.impala;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

public class GridSumJDBCConf {

    //Impala Memory Predict Service URL
    private static String impalaPredictURL;

    /**
     * Impala Query Retry Conf
     */
    private static int impalaRetryMaxCount = -1;
    private static int impalaRetryMemoryMultiple = -1;
    //unit GB
    private static String impalaRetryMaxMemory;
    //Impala OOM Exception String
    private static Vector<String> impalaOOMException = new Vector<String>();
    //Impala Query Detail Profile URL
    private static String impalaQueryProfile;


    /**
     * Connection Parameter Default Values
     */
    private static String predictMemPool;

    private static String impalaQueryPort;
    private static String cmApiUsername;
    private static String cmApiPassword;
    private static String cmApiClusterName;
    private static String cmApiImpalaServiceName;

    /**
     * backup server switch time out
     * unit second
     */
    private static int backupTimeOut = -1;


    static {
        try {
            load(null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void reLoad(String fileName) throws IOException {
        load(fileName);
    }

    private static void load(String fileName) throws IOException {
        InputStream stream = null;
        InputStreamReader streamReader = null;
        try {
            if (null != fileName) {
                stream = GridSumJDBCConf.class.getClassLoader().getResourceAsStream(fileName);
            } else {
                stream = GridSumJDBCConf.class.getClassLoader().getResourceAsStream("conf.properties");
            }
            if (null == stream) {
                throw new RuntimeException("You need to add 'conf.properties' in project resource folder.");
            }
            streamReader = new InputStreamReader(stream, "UTF-8");
            Properties properties = new Properties();
            properties.load(streamReader);
            impalaPredictURL = properties.getProperty("impala.predict.url");
            impalaRetryMaxCount = Integer.parseInt(properties.getProperty("impala.retry.max.count"));
            impalaRetryMemoryMultiple = Integer.parseInt(properties.getProperty("impala.retry.memory.multiple"));
            impalaRetryMaxMemory = properties.getProperty("impala.retry.max.memory");
            String[] exceptions = properties.getProperty("impala.oom.exception").split(",");
            for (String s : exceptions) {
                impalaOOMException.add(s);
            }
            impalaQueryProfile = properties.getProperty("impala.query.profile");
            predictMemPool = properties.getProperty("connection.default.impala.predict.pool");
            impalaQueryPort = properties.getProperty("connection.default.cm.query.port");
            cmApiUsername = properties.getProperty("connection.default.cm.username");
            cmApiPassword = properties.getProperty("connection.default.cm.password");
            cmApiClusterName = properties.getProperty("connection.default.cm.cluster.name");
            cmApiImpalaServiceName = properties.getProperty("connection.default.cm.impala.service.name");
            backupTimeOut = Integer.parseInt(properties.getProperty("backup.timeout"));
        } finally {
            if (streamReader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void addOOMException(String exceptionStr) {
        if (!impalaOOMException.contains(exceptionStr)) {
            impalaOOMException.add(exceptionStr);
        }
    }

    public static String getImpalaPredictURL() {
        if (null == impalaPredictURL) {
            throw new RuntimeException("You need to set 'impala.predict.url' value in 'conf.properties' in Resource folder. ");
        }
        return impalaPredictURL;
    }

    public static int getImpalaRetryMaxCount() {
        if (-1 == impalaRetryMaxCount) {
            throw new RuntimeException("You need to set 'impala.retry.max.count' value in 'conf.properties' in Resource folder. ");
        }
        return impalaRetryMaxCount;
    }

    public static int getImpalaRetryMemoryMultiple() {
        if (-1 == impalaRetryMemoryMultiple) {
            throw new RuntimeException("You need to set 'impala.retry.memory.multiple' value in 'conf.properties' in Resource folder. ");
        }
        return impalaRetryMemoryMultiple;
    }

    public static String getImpalaRetryMaxMemory() {
        if (null == impalaRetryMaxMemory) {
            throw new RuntimeException("You need to set 'impala.retry.max.memory' value in 'conf.properties' in Resource folder. ");
        }
        return impalaRetryMaxMemory;
    }

    public static List<String> getImpalaOOMException() {
        return impalaOOMException;
    }

    public static String getImpalaQueryProfile() {
        if (null == impalaQueryProfile) {
            throw new RuntimeException("You need to set 'impala.query.profile' value in 'conf.properties' in Resource folder. ");
        }
        return impalaQueryProfile;
    }

    public static int getBackupTimeOut() {
        if (-1 == backupTimeOut) {
            throw new RuntimeException("You need to set 'backup.timeout' value in 'conf.properties' in Resource folder. ");
        }
        return backupTimeOut;
    }

    public static String getPredictMemPool() {
        if (null == predictMemPool) {
            throw new RuntimeException("You need to set 'connection.default.impala.predict.pool' value in 'conf.properties' in Resource folder.");
        }
        return predictMemPool;
    }

    public static String getImpalaQueryPort() {
        if (null == impalaQueryPort) {
            throw new RuntimeException("You need to set 'connection.default.cm.query.port' value in 'conf.properties' in Resource folder. ");
        }
        return impalaQueryPort;
    }

    public static String getCmApiUsername() {
        if (null == cmApiUsername) {
            throw new RuntimeException("You need to set 'connection.default.cm.username' value in 'conf.properties' in Resource folder. ");
        }
        return cmApiUsername;
    }

    public static String getCmApiPassword() {
        if (null == cmApiPassword) {
            throw new RuntimeException("You need to set 'connection.default.cm.password' value in 'conf.properties' in Resource folder. ");
        }
        return cmApiPassword;
    }

    public static String getCmApiClusterName() {
        if (null == cmApiClusterName) {
            throw new RuntimeException("You need to set 'connection.default.cm.cluster.name' value in 'conf.properties' in Resource folder. ");
        }
        return cmApiClusterName;
    }

    public static String getCmApiImpalaServiceName() {
        if (null == cmApiImpalaServiceName) {
            throw new RuntimeException("You need to set 'connection.default.cm.impala.service.name' value in 'conf.properties' in Resource folder. ");
        }
        return cmApiImpalaServiceName;
    }
}
