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

import com.alibaba.fastjson.JSONObject;
import com.gridsum.impala.GridSumJDBCConf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
    private static final String HIVE_COMMAND_SET = "SET";
    private static final String IMPALA_DEL_TAIL = "Impala Version";//delete error message tail
    private static final Pattern MEM_LIMIT_PATTERN = Pattern.compile("MEM_LIMIT=(\\d*)");
    private static final Pattern STATEMENT_PATTERN = Pattern.compile("Sql\\s+Statement:\\s*([\\s\\S]*)\\s+Coordinator:");
    private static final Pattern QUERY_STATUS_PATTERN = Pattern.compile("Query Status:[\\s|\\S]*Impala Version");
    private static final Pattern QUERY_PATTERN = Pattern.compile("(?s)/\\*.*?\\*/");
    private static final Pattern BR_PATTERN = Pattern.compile("--[^\\n]*\\n");

    public static Long convertGB2Byte(String gb) {
        double size = Double.valueOf(gb);
        double byteNum = size * 1024 * 1024 * 1024;
        return (long) byteNum;
    }

    public static String convertMB2Byte(String mb) {
        double size = Double.valueOf(mb);
        double byteNum = size * 1024 * 1024;
        return String.valueOf((long) byteNum);
    }

    /**
     * Get mem_limit from query detail
     */
    public static Long getQueryMemLimit(String detail) {
        Matcher m = MEM_LIMIT_PATTERN.matcher(detail);
        return m.find() ? Long.parseLong(m.group(1)) : 0;
    }

    /**
     * Check sql is a query clause
     *
     * @param sql sql
     * @return boolean
     */
    public static boolean isQuery(String sql) {
        Matcher matcher = QUERY_PATTERN.matcher(sql);
        String temp = matcher.find() ? matcher.replaceAll("").toLowerCase().trim() : sql.toLowerCase().trim();
        matcher = BR_PATTERN.matcher(temp);
        temp = matcher.find() ? matcher.replaceAll("").trim() : temp.trim();
        if (temp.startsWith("with") || temp.startsWith("select") || temp.startsWith("(with") || temp.startsWith("(select")) {
            return !(temp.contains("insert into") || temp.contains("insert overwrite"));
        } else {
            return false;
        }
    }

    public static boolean isOOM(String queryStatus) {
        boolean isOOM = false;
        for (String s : GridSumJDBCConf.getImpalaOOMException()) {
            if (queryStatus.toLowerCase().contains(s)) {
                return true;
            }
        }
        return isOOM;
    }


    /**
     * Check sql is a set clause
     *
     * @param sql
     * @return
     */
    public static boolean isSet(String sql) {
        String[] tokens = tokenizeCmd(sql.trim());
        String cmd = tokens[0];
        if (cmd != null) {
            cmd = cmd.trim().toUpperCase();
            if (tokens.length > 1 && "role".equalsIgnoreCase(tokens[1])) {
                // special handling for set role r1 statement
                return false;
            } else if (tokens.length > 1 && "from".equalsIgnoreCase(tokens[1])) {
                //special handling for SQL "delete from <table> where..."
                return false;
            } else {
                return HIVE_COMMAND_SET.equals(cmd);

            }
        }
        return false;
    }

    /**
     * Get set clause key and value
     * Copy from Hive source code
     *
     * @param sql
     * @return
     */
    public static String[] getStatementProperty(String sql) {
        String firstToken = tokenizeCmd(sql.trim())[0];
        String cmd_1 = getFirstCmd(sql.trim(), firstToken.length());
        String nwcmd = cmd_1.trim();
        String[] part = new String[2];
        int eqIndex = nwcmd.indexOf('=');
        if (nwcmd.contains("=")) {
            if (eqIndex == nwcmd.length() - 1) { //x=
                part[0] = nwcmd.substring(0, nwcmd.length() - 1);
                part[1] = "";
            } else { //x=y
                part[0] = nwcmd.substring(0, eqIndex).trim();
                part[1] = nwcmd.substring(eqIndex + 1).trim();
            }
            return part;
        }
        return null;
    }

    /**
     * Copy from Hive source code
     */
    private static String[] tokenizeCmd(String cmd) {
        return cmd.split("\\s+");
    }

    /**
     * Copy from Hive source code
     */
    private static String getFirstCmd(String cmd, int length) {
        return cmd.substring(length).trim();
    }

    /**
     * Get query Statement from query detail
     */
    public static String getQueryStatement(String detail) {
        Matcher m = STATEMENT_PATTERN.matcher(detail);
        String statement = null;
        if (m.find()) {
            statement = m.group(1);
        }
        if (null != statement) {
            statement = replaceStatement(statement);
        }
        return statement;
    }

    /**
     * Fix query statement error that add too many '\'
     */
    private static String replaceStatement(String statement) {
        StringBuilder sb = new StringBuilder();
        int length = statement.length();
        for (int i = 0; i < length; i++) {
            char c = statement.charAt(i);
            if (c != '\\') {
                sb.append(c);
            } else {
                char cc = statement.charAt(++i);
                switch (cc) {
                    case 't':
                        sb.append("\t");
                        break;
                    case 'n':
                        sb.append("\n");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    default:
                        sb.append(cc);
                        break;
                }
            }
        }
        return sb.toString();
    }

    /**
     * add annotation for sql
     */
    public static String addAnnotation(String sql, JSONObject body) {
        return null != body ? sql + "\n" + ImpalaConstants.GS_ANNOTATION + body.toString() : sql;
    }

    public static int getRetryCountFromStatement(String statement) {
        int retryCount = 0;
        int lastIndex = statement.lastIndexOf(ImpalaConstants.GS_ANNOTATION);
        if (lastIndex != -1) {
            String jsonStr = statement.substring(lastIndex + ImpalaConstants.GS_ANNOTATION.length(), statement.length());
            JSONObject annotationJSON = JSONObject.parseObject(jsonStr);
            if (annotationJSON.containsKey(ImpalaConstants.RETRY_KEY)) {
                retryCount = annotationJSON.getInteger(ImpalaConstants.RETRY_KEY);
            }
        }
        return retryCount;
    }

    public static String getRetryStatement(String queryStatement, int retryCount) {
        JSONObject json = new JSONObject();
        int lastIndex = queryStatement.lastIndexOf(ImpalaConstants.GS_ANNOTATION);
        if (lastIndex != -1) {
            String jsonStr = queryStatement.substring(lastIndex + ImpalaConstants.GS_ANNOTATION.length(), queryStatement.length());
            json = JSONObject.parseObject(jsonStr.replaceAll("\\\\", ""));
            json.put(ImpalaConstants.RETRY_KEY, retryCount);
        } else {
            json.put(ImpalaConstants.RETRY_KEY, retryCount);
        }
        return (lastIndex == -1 ? queryStatement : queryStatement.substring(0, lastIndex))
                + "\n" + ImpalaConstants.GS_ANNOTATION + json.toString();
    }

    public static String getQueryStatus(String detail) {
        if (null != detail) {
            String temp = cutOutStr(detail);
            if (null != temp) {
                return temp.replace(IMPALA_DEL_TAIL, "").replace("\\n", "\n").trim();
            }
        }
        return null;
    }

    public static String cutOutStr(String target) {
        if (target == null) {
            throw new IllegalArgumentException("target is null");
        }
        Matcher matcher = QUERY_STATUS_PATTERN.matcher(target);
        return matcher.find() ? matcher.group() : null;
    }
}
