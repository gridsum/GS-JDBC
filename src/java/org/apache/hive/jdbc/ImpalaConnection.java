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

package org.apache.hive.jdbc;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v11.ServicesResourceV11;
import com.cloudera.api.v12.RootResourceV12;
import com.gridsum.impala.GridSumJDBCConf;
import com.gridsum.impala.util.ConnectionParams;
import com.gridsum.impala.util.HttpUtil;
import com.gridsum.impala.util.ImpalaConstants;
import com.gridsum.impala.util.StringUtil;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Impala Connection
 *
 */
public class ImpalaConnection extends HiveConnection {

    private static final Logger LOGGER = Logger.getLogger(ImpalaConnection.class);

    public ImpalaConnection(String uri, Properties info) throws SQLException {
        super(uri, info);
    }

    protected void openSession() throws SQLException {
        TOpenSessionReq openReq = new TOpenSessionReq();

        Map<String, String> openConf = new HashMap<String, String>();
        // for remote JDBC client, try to set the conf var using 'set foo=bar'
        for (Map.Entry<String, String> hiveConf : connParams.getHiveConfs().entrySet()) {
            openConf.put("set:hiveconf:" + hiveConf.getKey(), hiveConf.getValue());
        }
        // For remote JDBC client, try to set the hive var using 'set hivevar:key=value'
        for (Map.Entry<String, String> hiveVar : connParams.getHiveVars().entrySet()) {
            openConf.put("set:hivevar:" + hiveVar.getKey(), hiveVar.getValue());
        }
        // switch the database
        openConf.put("use:database", connParams.getDbName());

        // set the session configuration
        Map<String, String> sessVars = connParams.getSessionVars();
        if (sessVars.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
            openConf.put(HiveAuthFactory.HS2_PROXY_USER,
                    sessVars.get(HiveAuthFactory.HS2_PROXY_USER));
        }

        if (sessVars.containsKey(ConnectionParams.DELEGATION)) {
            openConf.put(ImpalaConstants.IMPALA_DELEGATION_KEY, sessVars.get(ConnectionParams.DELEGATION));
        }
        if (sessVars.containsKey(ConnectionParams.IMPALA_REQUEST_POOL)) {
            openConf.put(ConnectionParams.IMPALA_REQUEST_POOL, sessVars.get(ConnectionParams.IMPALA_REQUEST_POOL));
        }

        openReq.setConfiguration(openConf);

        // Store the user name in the open request in case no non-sasl authentication
        if (Utils.JdbcConnectionParams.AUTH_SIMPLE.equals(sessConfMap.get(Utils.JdbcConnectionParams.AUTH_TYPE))) {
            openReq.setUsername(sessConfMap.get(Utils.JdbcConnectionParams.AUTH_USER));
            openReq.setPassword(sessConfMap.get(Utils.JdbcConnectionParams.AUTH_PASSWD));
        }

        try {
            TOpenSessionResp openResp = client.OpenSession(openReq);

            // validate connection
            Utils.verifySuccess(openResp.getStatus());
            if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
                throw new TException("Unsupported Hive2 protocol");
            }
            protocol = openResp.getServerProtocolVersion();
            sessHandle = openResp.getSessionHandle();
        } catch (TException e) {
            LOG.error("Error opening session", e);
            throw new SQLException("Could not establish connection to "
                    + jdbcUriString + ": " + e.getMessage(), " 08S01", e);
        }
        isClosed = false;
    }

    public String getDbName() {
        return connParams.getDbName();
    }

    void setDbName(String dbName) {
        connParams.setDbName(dbName);
    }

    public String getPredictMemPool() {
        return getSessionValue(ConnectionParams.IMPALA_REQUEST_POOL, GridSumJDBCConf.getPredictMemPool());
    }


    public boolean hasPredictMemAuto() {
        boolean hasPredictMemAuto = false;
        if (sessConfMap.containsKey(ConnectionParams.IMPALA_PREDICT_MEM_AUTO)) {
            hasPredictMemAuto = Boolean.valueOf(sessConfMap.get(ConnectionParams.IMPALA_PREDICT_MEM_AUTO));
        }
        return hasPredictMemAuto;
    }

    //get CM API root
    private RootResourceV12 getApiRoot() throws MalformedURLException {
        if (sessConfMap.containsKey(ConnectionParams.CM_API_URL)) {
            LOGGER.info("Get cm root start.");
            RootResourceV12 root = new ClouderaManagerClientBuilder()
                    .withBaseURL(new URL(sessConfMap.get(ConnectionParams.CM_API_URL)))
                    .withUsernamePassword(this.getSessionValue(ConnectionParams.CM_API_USERNAME, GridSumJDBCConf.getCmApiUsername()),
                            this.getSessionValue(ConnectionParams.CM_API_PASSWORD, GridSumJDBCConf.getCmApiPassword()))
                    .build().getRootV12();
            LOGGER.info("Get cm root over.");
            return root;
        } else {
            throw new RuntimeException("CM_API_URL must be set in URL.");
        }
    }

    public int getRetryCount() {
        String retryCountStr = this.getSessionValue(ConnectionParams.IMPALA_RETRY_COUNT, "0");
        int retryCount = Integer.parseInt(retryCountStr);
        if (retryCount > GridSumJDBCConf.getImpalaRetryMaxCount()) {
            return GridSumJDBCConf.getImpalaRetryMaxCount();
        } else {
            return retryCount;
        }
    }

    public String getMemLimit() {
        if (sessConfMap.containsKey(ConnectionParams.IMPALA_MEL_LIMIT)) {
            return sessConfMap.get(ConnectionParams.IMPALA_MEL_LIMIT);
        } else {
            return null;
        }
    }

    public Long getMaxRetryMem() {
        String gb = getSessionValue(ConnectionParams.IMPALA_MAX_RETRY_MEM, GridSumJDBCConf.getImpalaRetryMaxMemory());
        return StringUtil.convertGB2Byte(gb);
    }

    private String getImpalaQueryPort() {
        if (sessConfMap.containsKey(ConnectionParams.IMPALA_QUERY_PORT)) {
            return sessConfMap.get(ConnectionParams.IMPALA_QUERY_PORT);
        } else {
            return GridSumJDBCConf.getImpalaQueryPort();
        }
    }

    /**
     * get queryDetail
     * get from impala 25000 service first,
     * if there is no query detail,
     * get from CM API.
     */
    public String getQueryDetail(String queryId) {
        String URL = HttpUtil.HTTP_PREFIX + this.host + ":" + getImpalaQueryPort() + GridSumJDBCConf.getImpalaQueryProfile();
        URL = String.format(URL, queryId);
        String response = null;
        try {
            LOGGER.info("Get query detail from 25000 start.");
            response = HttpUtil.doGet(URL);
            LOGGER.info("Get query detail from 25000 over.");
        } catch (RuntimeException | IOException e) {
            LOGGER.error(host + ":" + getImpalaQueryPort() + " is not available. Cause by:" + e.toString());
            return getQueryDetailByCM(queryId);
        }
        if (null != response) {
            JSONObject json = JSONObject.parseObject(response);
            if (json.containsKey(ImpalaConstants.ERROR_KEY)) {
                LOGGER.info("Get query detail from impala server error.QueryDetail is " + response);
                return getQueryDetailByCM(queryId);
            }
            return response;
        } else {
            return getQueryDetailByCM(queryId);
        }
    }

    /**
     * Get queryDetail from CM API
     */
    private String getQueryDetailByCM(String queryId) {
        String queryDetail = null;
        try {
            LOGGER.info("Get query detail from cm api start.");
            ServicesResourceV11 servicesResource = getApiRoot().getClustersResource().
                    getServicesResource(this.getSessionValue(ConnectionParams.CM_API_CLUSTER_NAME, GridSumJDBCConf.getCmApiClusterName()));
            queryDetail = servicesResource.getImpalaQueriesResource(this.getSessionValue(ConnectionParams.CM_API_IMPALA_SERVICE_NAME, GridSumJDBCConf.getCmApiImpalaServiceName()))
                    .getQueryDetails(queryId, "text").getDetails();
            LOGGER.info("Get query detail from cm api over.");
        } catch (Exception e) {
            LOGGER.error("CM_API is not available.Please check parameter <CM_API_URL;CM_API_USERNAME;CM_API_PASSWORD;CM_API_CLUSTER_NAME;CM_API_IMPALA_SERVICE_NAME> in URL. Cause by:"
                    + e.toString());
        }
        return queryDetail;
    }

    public String getDelegationToken(String owner, String renewer) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void cancelDelegationToken(String tokenStr) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void renewDelegationToken(String tokenStr) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can't create Statement, connection is closed");
        }
        return new ImpalaStatement(this, client, sessHandle);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException("Statement with resultset concurrency " +
                    resultSetConcurrency + " is not supported", "HYC00"); // Optional feature not implemented
        }
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
            throw new SQLException("Statement with resultset type " + resultSetType +
                    " is not supported", "HYC00"); // Optional feature not implemented
        }
        return new ImpalaStatement(this, client, sessHandle,
                resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
    }
}
