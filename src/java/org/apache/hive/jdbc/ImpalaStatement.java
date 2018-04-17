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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gridsum.impala.GridSumJDBCConf;
import com.gridsum.impala.memory.predict.PredictService;
import com.gridsum.impala.memory.predict.parameter.QueryParam;
import com.gridsum.impala.util.ConnectionParams;
import com.gridsum.impala.util.ImpalaConstants;
import com.gridsum.impala.util.StringUtil;
import org.apache.hive.service.cli.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Impala Statement
 *
 */
public class ImpalaStatement extends HiveStatement {

    private static final Logger LOGGER = Logger.getLogger(HiveStatement.class);

    public ImpalaStatement(HiveConnection connection, TCLIService.Iface client, TSessionHandle sessHandle) {
        super(connection, client, sessHandle);
    }

    public ImpalaStatement(HiveConnection connection, TCLIService.Iface client, TSessionHandle sessHandle, boolean isScrollableResultset) {
        super(connection, client, sessHandle, isScrollableResultset);
    }

    /**
     * Get queryId
     */
    private String getQueryId() {
        StringBuilder sb = new StringBuilder();
        org.apache.thrift.TBaseHelper.toString(ByteBuffer.wrap(stmtHandle.getOperationId().getGuid()), sb);
        String guid = sb.toString();
        String[] tokens = guid.split(" ");
        StringBuilder result = new StringBuilder();
        /**
         * transform guid to queryId
         * Example : guid F2 7A A4 98 46 F3 41 33 B7 A7 28 AA 39 C9 B9 A9 transform to queryId 3341f34698a47af2:a9b9c939aa28a7b7
         */
        for (int i = tokens.length / 2 - 1; i >= 0; i--) {
            result.append(tokens[i].toLowerCase());
        }
        result.append(":");
        for (int i = tokens.length - 1; i >= tokens.length / 2; i--) {
            result.append(tokens[i].toLowerCase());
        }
        return result.toString();
    }

    /**
     * execute sql donot need return resutlSet
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    private boolean executeSQL(String sql) throws SQLException {
        checkConnection("execute");

        try {
            closeClientOperation();
        } catch (SQLException eS) {
            if (eS.toString().contains(ImpalaConstants.TTRANSPORT_EXCEPTION)) {
                LOGGER.info("Catch close operation error.");
                reInitializeStmt();
            }
            isExecuteStatementFailed = true;
            throw eS;
        }
        initFlags();

        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, sql);
        /**
         * Run asynchronously whenever possible
         * Currently only a SQLOperation can be run asynchronously,
         * in a background operation thread
         * Compilation is synchronous and execution is asynchronous
         */
        execReq.setRunAsync(true);
        execReq.setConfOverlay(sessConf);

        try {
            TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
            Utils.verifySuccessWithInfo(execResp.getStatus());
            stmtHandle = execResp.getOperationHandle();
            isExecuteStatementFailed = false;
        } catch (SQLException eS) {
            if (eS.toString().contains(ImpalaConstants.CATALOG_UPDATE_EXCEPTION)) {
                LOGGER.info("Catch waiting catalog update error.");
                reInitializeStmt();
            }
            isExecuteStatementFailed = true;
            throw eS;
        } catch (TTransportException te) {
            LOGGER.info("Catch execute sql error.");
            reInitializeStmt();
            isExecuteStatementFailed = true;
            throw new SQLException(te.toString(), "08S01", te);
        } catch (Exception ex) {
            isExecuteStatementFailed = true;
            throw new SQLException(ex.toString(), "08S01", ex);
        }

        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle);
        boolean operationComplete = false;
        TGetOperationStatusResp statusResp;

        // Poll on the operation status, till the operation is complete
        while (!operationComplete) {
            try {
                /**
                 * For an async SQLOperation, GetOperationStatus will use the long polling approach
                 * It will essentially return after the HIVE_SERVER2_LONG_POLLING_TIMEOUT (a server config) expires
                 */
                statusResp = client.GetOperationStatus(statusReq);
                Utils.verifySuccessWithInfo(statusResp.getStatus());
                if (statusResp.isSetOperationState()) {
                    switch (statusResp.getOperationState()) {
                        case CLOSED_STATE:
                        case FINISHED_STATE:
                            operationComplete = true;
                            break;
                        case CANCELED_STATE:
                            // 01000 -> warning
                            throw new SQLException("Query was cancelled", "01000");
                        case ERROR_STATE:
                            int retryCount = ((ImpalaConnection) this.connection).getRetryCount();
                            if (retryCount > 0) {
                                LOGGER.info("Ready for retry.");
                                String queryId = this.getQueryId();
                                String queryDetail = ((ImpalaConnection) this.connection).getQueryDetail(queryId);
                                String queryStatus = StringUtil.getQueryStatus(queryDetail);
                                if (null != queryStatus && StringUtil.isOOM(queryStatus)) {
                                    String queryStatement = StringUtil.getQueryStatement(queryDetail);
                                    if (null != queryStatement) {
                                        int currentRetryCount = StringUtil.getRetryCountFromStatement(queryStatement) + 1;
                                        String retrySQL = StringUtil.getRetryStatement(queryStatement, currentRetryCount);
                                        if (StringUtil.isQuery(retrySQL)) {
                                            Long memLimit = StringUtil.getQueryMemLimit(queryDetail);
                                            memLimit = memLimit * GridSumJDBCConf.getImpalaRetryMemoryMultiple();
                                            //when retry count less than max retry count and retry mem_limit less than max retry mem_limit , it will execute the sql again.
                                            if (currentRetryCount <= GridSumJDBCConf.getImpalaRetryMaxCount() && memLimit <= ((ImpalaConnection) this.connection).getMaxRetryMem()) {
                                                LOGGER.info("Set retry memory start.");
                                                executeSQL(String.format(ImpalaConstants.MEM_LIMIT, memLimit.toString()));
                                                LOGGER.info("Set retry memory over.");
                                                LOGGER.info("Execute retry sql start.");
                                                executeSQL(retrySQL);
                                                LOGGER.info("Execute retry sql over.");
                                                operationComplete = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            if (null == statusResp.getErrorMessage()) {
                                LOGGER.info("Original error message is null.Get query status start.");
                                String queryId = this.getQueryId();
                                String queryDetail = ((ImpalaConnection) this.connection).getQueryDetail(queryId);
                                String queryStatus = StringUtil.getQueryStatus(queryDetail);
                                LOGGER.info("Get query status over.");
                                if (null != queryStatus) {
                                    throw new SQLException(queryStatus,
                                            statusResp.getSqlState(), statusResp.getErrorCode());
                                } else {
                                    LOGGER.info("QueryStatus is null.QueryDetail is " + queryDetail);
                                    throw new SQLException("query has already canceled, please contact administrator.",
                                            statusResp.getSqlState(), statusResp.getErrorCode());
                                }
                            }
                            // Get the error details from the underlying exception
                            throw new SQLException(statusResp.getErrorMessage(),
                                    statusResp.getSqlState(), statusResp.getErrorCode());
                        case UKNOWN_STATE:
                            throw new SQLException("Unknown query", "HY000");
                        case INITIALIZED_STATE:
                        case PENDING_STATE:
                        case RUNNING_STATE:
                            break;
                    }
                }
            } catch (SQLException e) {
                isLogBeingGenerated = false;
                throw e;
            } catch (TTransportException te) {
                LOGGER.info("Catch executing sql error.");
                reInitializeStmt();
                isExecuteStatementFailed = true;
                throw new SQLException(te.toString(), "08S01", te);
            } catch (Exception e) {
                isLogBeingGenerated = false;
                throw new SQLException(e.toString(), "08S01", e);
            }
        }

        isLogBeingGenerated = false;

        // The query should be completed by now
        if (!stmtHandle.isHasResultSet()) {
            return false;
        }

        return true;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (StringUtil.isSet(sql)) {
            String[] setStatementKeyValue = StringUtil.getStatementProperty(sql);
            if (null != setStatementKeyValue) {
                String key = setStatementKeyValue[0];
                String value = setStatementKeyValue[1];
                for (String parameter : ConnectionParams.PARAMETERS) {
                    if (key.toUpperCase().equals(parameter)) {
                        this.connection.putSessionValue(parameter, value);
                        return true;
                    }
                }
                if (key.toUpperCase().equals(ConnectionParams.DB)) {
                    ((ImpalaConnection) this.connection).setDbName(value);
                    return true;
                }
                if (key.toUpperCase().equals(ConnectionParams.IMPALA_REQUEST_POOL)) {
                    this.connection.putSessionValue(ConnectionParams.IMPALA_REQUEST_POOL, value);
                }
            }
        }
        // set mem when need
        JSONObject memBody = null;
        String memSetting = null;
        if (StringUtil.isQuery(sql)) {
            if (((ImpalaConnection) this.connection).hasPredictMemAuto()) {
                QueryParam queryParam = new QueryParam();
                String db = ((ImpalaConnection) this.connection).getDbName();
                queryParam.setSql(sql).setDb(db).setPool(((ImpalaConnection) this.connection).getPredictMemPool());
                try {
                    LOGGER.info("Ready for get predict memory.");
                    memBody = JSON.parseObject(PredictService.getPredictResponse(queryParam));
                    memSetting = PredictService.getPredictMemory(memBody);
                } catch (RuntimeException e) {
                    LOGGER.error("Get predict memory exception:", e);
                    memBody = new JSONObject();
                    memBody.put(ImpalaConstants.ERROR_KEY, e.toString());
                }
                if (null != memSetting) {
                    executeSQL(String.format(ImpalaConstants.MEM_LIMIT, StringUtil.convertMB2Byte(memSetting)));
                }
            } else {
                memSetting = ((ImpalaConnection) this.connection).getMemLimit();
                if (null != memSetting) {
                    executeSQL(String.format(ImpalaConstants.MEM_LIMIT, memSetting));
                }
            }
        }
        String str = StringUtil.addAnnotation(sql, memBody);
        if (!executeSQL(str)) {
            return false;
        }
        resultSet = new HiveQueryResultSet.Builder(this).setClient(client).setSessionHandle(sessHandle)
                .setStmtHandle(stmtHandle).setMaxRows(maxRows).setFetchSize(fetchSize)
                .setScrollable(isScrollableResultset)
                .build();
        return true;
    }
}
