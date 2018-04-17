# 1.Introduction

GS-JDBC is a composite JDBC, which provides JDBC for Hive and Impala. In addition, GS-JDBC provides some additional feature.

# 2. Installation

You can use GS-JDBC by these ways:

- Download the source code, compile it by Maven, and use the jar.

# 3. How to use GS-JDBC

## 3.1 Precondition

- System Requirements

  - JDK ( >= 1.7 )
  - Maven ( >=3 )
  - Impala ( >=impala-2.5.0+cdh5.7.2 )
  - Cloudera Manager ( >=cdh5.7.2 )
  - Hive ( >=hive-1.1.0+cdh5.7.2 )

- Optional Service

   - IML-Predictor (More information about IML-Predictor, please see URL)

- Conf File

  - Add ```conf.properties``` Conf File in your project ```Resource``` folder, with the following content:

  - You can use ```GridSumJDBCConf.reLoad("myconf.properties")``` function to reload another conf file.


    ```
    # IML-Predictor service API URL for Memory Predict feature
    
    impala.predict.url=http://10.202.42.2:8889/v1/impala/memory/predict
    
    # following properties usually do not be change 
    
    # max retry count for OOM Retry feature
    impala.retry.max.count=3
    # mem_limit multiple for OOM Retry feature
    impala.retry.memory.multiple=2
    # max retry memory for OOM Retry feature, units GB
    impala.retry.max.memory=5
    # out of memory exception string for OOM Retry feature
    impala.oom.exception=memory limit exceeded,cannot perform hash join,cannot perform aggregate join
    # Impala service api URL, you can get query detail
    impala.query.profile=/query_profile?json&query_id=%s
    # both master and backup server invalid timeout for Backup Server feature
    backup.timeout=30
    
    # you can set following property when you create Connection URL , so they are start with 'connection'
    
    # default pool for Memory Predict feature
    connection.default.impala.predict.pool=default
    # Impala service port
    connection.default.cm.query.port=25000
    # Cloudera Manager login username 
    connection.default.cm.username=guest
    # Cloudera Manager login user password
    connection.default.cm.password=cmguest
    # Cloudera Manager cluster name
    connection.default.cm.cluster.name=cluster
    # Cloudera Manager impala service name
    connection.default.cm.impala.service.name=impala
    
    ```

## 3.2 How to use

GS-JDBC just like standard Hive/Impala JDBC API, we need to set ***Driver Class*** and ***Connection URL***.


### 3.2.1 Driver Class

- For Hive and Impala service, GS-JDBC provide two Driver Class.
  - Hive Driver Class : ```org.apache.hive.jdbc.HiveDriver```
  - Impala Driver Class : ```org.apache.hive.jdbc.ImpalaDriver```

### 3.2.2 Connection URL

- GS-JDBC use the same ***Subprotocol*** to connect Hive and Impala service, you can see the following standard JDBC Connection URL. 
- And you can use GS-JDBC all features through set the right property in the Connection URL.
- About Connection URL Properties, you can see : [Connection URL property list](#34-connection-url-property-list)
- ```jdbc:Subprotocol://Host:Port[/Schema];Property1=Value;Property2=Value;...```

### 3.2.3 Java Sample Code
 -   ```java
        import java.sql.*

        //Hive 
        //URL include GS-JDBC features
        String URL = "jdbc:hive2://192.168.1.1:10000/default;BACK_UP=192.168.1.2;DelegationUID=SomeOne";

        String SQL = "select count(*) from db.table";

        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }

        rs.close();
        stmt.close();
        
        //Impala
        //URL include GS-JDBC features
        URL = "jdbc:hive2://192.168.1.1:21050/default;BACK_UP=192.168.1.2;DelegationUID=SomeOne" 
        + "REQUEST_POOL=MyPool;MEM_LIMIT=300mb;RETRY_COUNT=3;MAX_RETRY_MEM=5;PREDICT_MEM_AUTO=true;DB=default";

        String SQL = "select count(*) from db.table";

        driverName = "org.apache.hive.jdbc.ImpalaDriver";
        Class.forName(driverName);
        con = DriverManager.getConnection(URL);
        stmt = con.createStatement();
        rs = stmt.executeQuery(SQL);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }
        rs.close();
        stmt.close();

    ```


## 3.3 GS-JDBC Features


### 3.3.1 Impala Features

- Delegation User

  - Used to delegate all operations against Impala to a user that is different than the authenticated user for the connection.

  - Set ***DelegationUID*** in the Connection URL to use this feature.

  - example: ```jdbc:hive2://192.168.1.1:21050/default;DelegationUID=SomeOne```


- Set REQUEST_POOL

  - Set REQUEST_POOL when you create Connection, to lessen once operator to service.

  - Set ***REQUEST_POOL*** in the Connection URL to use this feature.

  - example: ```jdbc:hive2://192.168.1.1:21050/default;REQUEST_POOL=MyPool```


- Memory Predict

  - It is a feature need ***IML-Predictor***, if you want to use this feature, you must install ***IML-Predictor***.(More information about IML-Predictor, please see URL)

  - Before you execute a query statement, GS-JDBC call ***IML-Predictor*** service first, and ***IML-Predictor*** will return appropriate ***mem_limit*** to insure the query statement can be finish and use least memory.

  - Set ***DB*** and ***PREDICT_MEM_AUTO*** in the Connection URL to use this feature.


  - The properties be related to this feature in ```conf.properties``` file:

       - impala.predict.url

  - The properties be related to this feature in Connection URL:

       - DB：Set the right database before you execute query.
       - PREDICT_MEM_AUTO： Setting this property to true will have the Connection execute every query statement use Memory Predict feature. 


  - example: ```jdbc:hive2://192.168.1.1:21050/default;PREDICT_MEM_AUTO=true;DB=default```

- Get Exception Detail

  - This feature provides a way to get exception details, which is not supported in the standard Impala JDBC API.

  - When you use the right version of Impala service, you can use this feature direct, properties in the ```conf.properties``` file can work without any change. But we suggest you know the properties and set them to the right value, it can make sure this feature work better.

  - It will get query detail from Impala Service (port 25000 as default) first , if Impala Service is invalid, it will try to get query detail from Cloudera Manager.

  - **Important: This feature is the base of the OOM Retry feature, If this feature not work, OOM Retry will not work too.**


  - The properties be related to this feature in ```conf.properties``` file:

       - impala.query.profile=/query_profile?json&query_id=%s
       - connection.default.cm.query.port=25000
       - connection.default.cm.username=guest
       - connection.default.cm.password=cmguest
       - connection.default.cm.cluster.name=cluster
       - connection.default.cm.impala.service.name=impala

  - The properties be related to this feature in Connection URL: 

       - CM_API_HOST: Cloudera Manager Service URL, if you want this feature can get query detail from Cloudera Manager, then you must set this property.
       - CM_API_USERNAME: Cloudera Manager login username.
       - CM_API_PASSWORD: Cloudera Manager login user password.
       - CM_API_CLUSTER_NAME: Cloudera Manager cluster name.
       - CM_API_IMPALA_SERVICE_NAME: Cloudera Manager impala service name.
       - IMPALA_QUERY_PORT: Impala Service port.


- OOM Retry

  - GS-JDBC will check every failed query statement, if the reason is Out Of Memory(OOM), GS-JDBC will increase ***MEM_LIMIT*** value and retry execute query statement.

  - This feature only work for query statement, and it only work for the OOM statement that happens in running status.

  - There are some properties in ```conf.properties``` to control the max retry count and the max retry memory.

  - And GS-JDBC provide ```GridSumJDBCConf.addOOMException("new oom expection string");``` function to add new OOM exception string.


  - The properties be related to this feature in ```conf.properties``` file:

       - impala.retry.max.count
       - impala.retry.memory.multiple
       - impala.retry.max.memory
       - impala.oom.exception

  - The properties be related to this feature in Connection URL:

       - IMPALA_MAX_RETRY_MEM: The max retry memory , units GB.
       - IMPALA_RETRY_COUNT: The max retry count, must smaller than ```impala.retry.max.count```.

  - example：```jdbc:hive2://192.168.1.1:21050/;IMPALA_MAX_RETRY_MEM=3;IMPALA_RETRY_COUNT=3```


- Set MEM_LIMIT

  - Set MEM_LIMIT when you create Connection, it will be use when **without Memory Predict feature**.


  - Set ***MEM_LIMIT*** in the Connection URL to use this feature.


  - You can set value like MEM_LIMIT=500mb/500MB/2g/2G/2gb/2GB.

  - example: ```jdbc:hive2://192.168.1.1:21050/default;MEM_LIMIT=500mb```

- Backup Server

  - When the connected Impala Server error, this feature can change to the Backup Impala Server for the next statement.

  - This feature will not retry execute for the failed statement.

  - Set ***BACK_UP*** in the Connection URL to use this feature.


  - The properties be related to this feature in ```conf.properties``` file:

       - backup.timeout=30


  - The properties be related to this feature in Connection URL:

       - BACK_UP: Backup impala server host.

  - example: ```jdbc:hive2://192.168.1.1:21050/default;BACK_UP=192.168.1.2```


- Support Set Mode

  - For Some Impala Feature, GS-JDBC use them not only with set property in the Connection URL,but also with execute ***SET Property = Value*** statement.


  - Set Mode Porperties：

       - BACK_UP
       - DB
       - MEM_LIMIT
       - PREDICT_MEM_CLUSTER
       - PREDICT_MEM_USER
       - PREDICT_MEM_AUTO
       - MAX_RETRY_MEM
       - CM_API_HOST
       - CM_API_USERNAME
       - CM_API_PASSWORD
       - CM_API_CLUSTER_NAME
       - CM_API_IMPALA_SERVICE_NAME
       - IMPALA_QUERY_PORT
       - RETRY_COUNT

  - example：

    ```java

        import java.sql.*

       
        //Impala
        String URL = "jdbc:hive2://192.168.1.1:21050/default;";
       

        String SQL = "select count(*) from db.table";

        driverName = "org.apache.hive.jdbc.ImpalaDriver";
        Class.forName(driverName);
        con = DriverManager.getConnection(URL);
        stmt = con.createStatement();
        //Set Mode
        stmt.execute("set REQUEST_POOL='MyPool'");
        stmt.execute("set MEM_LIMIT=300mb");
        stmt.execute("set RETRY_COUNT=3");
        stmt.execute("set MAX_RETRY_MEM=5");
        stmt.execute("set BACK_UP=192.168.1.2");
        stmt.execute("set PREDICT_MEM_AUTO=true");
        stmt.execute("set DB=ad");
        stmt.execute("set CM_API_URL=https://192.168.1.1:7180");
        rs = stmt.executeQuery(SQL);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }
        rs.close();
        stmt.close();

    ```


### 3.3.2 Hive Features

- Delegation User

  - Used to delegate all operations against Hive to a user that is different than the authenticated user for the connection

  - Set ***DelegationUID*** in the Connection URL to use this feature.

  - example: ```jdbc:hive2://192.168.1.1:10000/default;DelegationUID=SomeOne```


- Backup Server

  - When the connected Hive Server error, this feature can change to the Backup Impala Coordinator for the next statement.

  - This feature will not retry execute for the failed statement.

  - Set ***BACK_UP*** in the Connection URL to use this feature.

  - The properties be related to this feature in ```conf.properties``` file:

       - backup.timeout=30

  - The properties be related to this feature in Connection URL:

       - BACK_UP: Backup hive server host.

  - example: ```jdbc:hive2://192.168.1.1:10000/default;BACK_UP=192.168.1.2```

- Support Set Mode

  - For Some Hive Feature, GS-JDBC use them not only with set property in the Connection URL,but also with execute  ***SET Property = Value*** statement.

  - Set Mode Porperties：

       - BACK_UP
      
  - example：

    ```java

        import java.sql.*

        //Hive 
        String URL = "jdbc:hive2://192.168.1.1:10000/default;";

        String SQL = "select count(*) from db.table";

        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        stmt.execute("set BACK_UP=gs-server-1040");
        ResultSet rs = stmt.executeQuery(SQL);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }

        rs.close();
        stmt.close();
        

    ```


## 3.4 Connection URL Property List

|Property                               |Default                 |Use Service     |Description                             
| -                                | :-:                     | :-:        | :-:                                 
| DelegationUID                    | N/A                     |Impala/Hive |Used to delegate all operations against Hive/Impala to a user that is different than the authenticated user for the connection
| REQUEST_POOL                     | N/A                     |Impala      |Set REQUEST_POOL when you create Connection 
| DB                               | default                 |Impala      |Set the right database before you execute query
| PREDICT_MEM_AUTO                 | N/A                     |Impala      |Boolean,Setting this property to true will have the Connection execute every query statement use Memory Predict feature
| IMPALA_MAX_RETRY_MEM             | 5                       |Impala      |The max retry memory , units GB
| IMPALA_RETRY_COUNT               | 3                       |Impala      |The max retry count, must smaller than impala.retry.max.count
| CM_API_HOST                      | N/A                     |Impala      |Cloudera Manager Service URL, if you want this feature can get query detail from Cloudera Manager, then you must set this property
| CM_API_USERNAME                  | N/A                     |Impala      |Cloudera Manager login username
| CM_API_PASSWORD                  | N/A                     |Impala      |Cloudera Manager login user password
| CM_API_CLUSTER_NAME              | N/A                     |Impala      |Cloudera Manager cluster name
| CM_API_IMPALA_SERVICE_NAME       | N/A                     |Impala      |Cloudera Manager impala service name
| IMPALA_QUERY_PORT                | N/A                     |Impala      |Impala Service port
| MEM_LIMIT                        | N/A                     |Impala      |Set MEM_LIMIT when you create Connection, it will be use when **without Memory Predict feature**.
| BACK_UP                          | N/A                     |Impala/Hive      |Backup Hive/Impala server host

# 4. Communication

impala-toolbox-help@gridsum.com

# 5. License

IPM-Scheduler is [licensed under the Apache License 2.0.](./LICENSE)
