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
package com.gridsum.impala.memory.predict;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gridsum.impala.GridSumJDBCConf;
import com.gridsum.impala.memory.predict.parameter.ParamNameEnum;
import com.gridsum.impala.memory.predict.parameter.QueryParam;
import com.gridsum.impala.util.HttpUtil;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PredictService {

    private static final Logger LOGGER = Logger.getLogger(PredictService.class);

    /**
     * Post Request Success
     */
    private static final String SUCCESS_STATUS = "0";

    /**
     * Convert QueryParam to JSON
     */
    public static JSONObject genParam(QueryParam queryParam) {
        JSONObject param = new JSONObject();
        param.put(ParamNameEnum.SQL.getName(), queryParam.getSql());
        param.put(ParamNameEnum.DB.getName(), queryParam.getDb());
        param.put(ParamNameEnum.POOL.getName(), queryParam.getPool());
        return param;
    }

    public static String getPredictMemory(JSONObject json) {
        return json.getString(ParamNameEnum.MEM.getName());
    }

    /**
     * Get Memory Predict Service Response
     *
     * @param queryParam query parameter
     */
    public static String getPredictResponse(QueryParam queryParam) {

        String url = GridSumJDBCConf.getImpalaPredictURL();
        if (url == null) {
            throw new RuntimeException(
                    "conf.properties has not been init, please check the file exist or not.");
        }
        JSONObject bodyJson = null;
        try {
            LOGGER.info("Get predict memory start.");
            String body = HttpUtil.doPost(url, genParam(queryParam));
            bodyJson = JSON.parseObject(body);
            if (!SUCCESS_STATUS.equals(bodyJson.getString(ParamNameEnum.ERROR_CODE.getName()))) {
                throw new RuntimeException("get memory setting occur an error:" + bodyJson.toJSONString());
            }
            LOGGER.info("Get predict memory over.");
        } catch (IOException e) {
            throw new RuntimeException("get memory setting occur an error:" + e.toString());
        }
        return bodyJson.toString();
    }
}
