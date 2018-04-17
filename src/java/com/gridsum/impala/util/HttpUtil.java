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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gridsum.impala.memory.predict.parameter.ParamNameEnum;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class HttpUtil {
    private static final String CONTENT_TYPE = "application/json";
    public static final String HTTP_PREFIX = "http://";
    private static final int HTTP_TIME_OUT = 5000;//unit ms
    /**
     * Memory predict service response error code
     */
    private static final int USER_ERROR_CODE = 400;
    private static final int SERVICE_ERROR_CODE = 500;

    /**
     * Send post request to memory predict service
     */
    public static String doPost(String url, JSONObject body) throws IOException {
        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }
        String result = "";
        CloseableHttpClient httpClient = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", CONTENT_TYPE);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(HTTP_TIME_OUT).setConnectTimeout(HTTP_TIME_OUT).build();
            httpPost.setConfig(requestConfig);
            httpClient = HttpClients.createDefault();
            if (!body.isEmpty()) {
                StringEntity entity = new StringEntity(body.toJSONString(), Charset.forName("utf-8"));
                entity.setContentType(CONTENT_TYPE);
                httpPost.setEntity(entity);
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case HttpStatus.SC_OK:
                    result = EntityUtils.toString(response.getEntity(), "utf-8");
                    break;
                case USER_ERROR_CODE:
                case SERVICE_ERROR_CODE:
                    result = EntityUtils.toString(response.getEntity(), "utf-8");
                    JSONObject json = JSON.parseObject(result);
                    if (json.containsKey(ParamNameEnum.MESSAGE_KEY.getName())) {
                        result = json.getString(ParamNameEnum.MESSAGE_KEY.getName());
                    }
                    throw new RuntimeException("HttpStatus is " + statusCode + ", errorMessage is " + result);
                default:
                    throw new RuntimeException("HttpStatus is " + statusCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (null != httpClient) {
                httpClient.close();
            }
        }
        return result;
    }

    /**
     * Send get request
     */
    public static String doGet(String url) throws IOException {
        if (url == null) {
            throw new IllegalArgumentException("url is null");
        }
        String result = "";
        CloseableHttpClient httpClient = null;
        try {
            HttpGet request = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(HTTP_TIME_OUT).setConnectTimeout(HTTP_TIME_OUT).build();
            request.setConfig(requestConfig);
            httpClient = HttpClients.createDefault();
            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case HttpStatus.SC_OK:
                    result = EntityUtils.toString(response.getEntity(), "utf-8");
                    break;
                default:
                    throw new RuntimeException("HttpStatus is " + statusCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (null != httpClient) {
                httpClient.close();
            }
        }
        return result;
    }

}
