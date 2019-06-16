# Mycat 2

Proxy-centric high-performance MySQL Middleware.

[中文文档](/doc/00-mycat-readme.md)

[任务](/doc/101-todo-history-list.md)

[mysql-packet-parsing-state-machine](/doc/100-mysql-packet-parsing-state-machine.md)

## Advantage

1. exchange data using a fixed-size network buffer as a  streaming proxy from backend to front that called passthrough.
2. may be as another asynchronous client  of mysql
3. may be as another mysql server framework

## Some limits of functions

- passthrough can not run on the backend under compression protocol,because payload  must decompression.
- still working hard to develop...

## Roadmap

### First stage of development

Develop a mysql high-performance proxy that simply parses the request and forwards the response. 

Mycat proxy does not handle complex requests that will be implemented in the second phase of development.

### Second stage of development

Implement more router and part of the database function in Mycat node. Mycat proxy forwards the response of Mycat node to the client.







## Configuration 

the kinds of configuration that don't changed frequently.

### mycat config

- #### ip


the ip of mycat server 

- #### port


the port of mycat server 

- ### replicas


a replica treated as a consistent mysql internal load balancing.

- #### replica -name


the the name of replica that can be reference by data node config in schema config

- #### repType


type of replica:x:

- #### balanceName


reference load balance name that be in plug config

- #### mysqls


configure multiple mysql connection config

#### mysqls-mysql

- ###### name

  the the name of mysql connection info

- ###### ip

  the ip of mysql server 

- ###### port

  the port of mysql server 

- ###### user

  username of mysql user

- ###### password

  password of mysql user

- ###### minCon

  the number of init mysql connection

- ###### maxCon

  the number of limit mysql connection




### masterIndexes config

datasource switch record index

```
masterIndexes:
  repli: 0
  repli2: 0
```

repli is a replica  name that in datasource config

the number 0 is mysqls index  in datasource confg marks as matser mysql server

when the master node switches, the number changes to new master index



### schema config

#### router abstract

- ##### schema

A logic schema in mycat that represents how different tables are organized.

NOTE:A client only send sql or initDb command to switch it and mycat do not support SQL that contains schema.



1. ###### DB IN ONE SERVER

  All table are in same mysql server.It routes SQL by  current schema in proxy session not based on SQL.

  It means mysql client can switch data node by 'use {schema}' .Generally,there is no difference between using a data node and using MySQL server directly.

2. ###### DB IN MULTI SERVER

   A table corresponds to a data node.It routes SQL by a table name in sql.And this table must exist in the current schema.It means mysql client can switch data node by a table name in SQL.It supports only one table operation.

3. ###### ANNOTATION ROUTE

   In general,use a manually pattern match, such as regular expressions, to extract information and select data node.Further more,mycat provides an case of annotated routing.

   A table contains multiple data nodes.

   A table have a partitioning algorithm,supporting two functions.The first is the ability to select a node based on a value.The second is the ability to select a set of data nodes based on a range value.

   A table have a rule to extract a value or two values to represent a continuous range only on a data node.We can choose regular expressions to implement this extraction rule.

   In this case,there are some limitations to simplify the annotation routing.

   1. The current schema and only a table name in SQL to determine the table.
   2. If continuous values on multiple data nodes is not supported in mycat proxy.Because It needs to split SQL and merge to process result set of multiple nodes. This is not suitable for processing in the proxy.

   

4. ###### SQL PARSE ROUTE:x:

   It  try to make a mysql proxy like a real mysql server.



- ##### table

a logic table in mycat.Corresponding to the table on the mysql server, we call it the physical table.

- Generally, its name is unique in all schemas. When SQL is received, mycat can route it and process according to that name.

- The logic table name must correspond to the name of the physical table name(Although tablename can be rewritten to support it).

- For SQL without a table name, Mycat responds to the SQL itself or sends it to the default data node.

  

  ###### table type

  - DEFAULT 

    when schema type is DB IN ONE SERVER,the  type of table in the schema is dafault.The router select 

    data node by the property 'dafaultDataNode' of schema.

    when schema type is DB IN MULTI SERVER,the  type of table in the schema also is dafault.The router select data node by first data node name in the property 'dataNodes' of table.

  - SHARING DATABASE

    when schema is ANNOTATION ROUTE or SQL PARSE ROUTE,it support SHARING DATABASE.it select a data node which is with  a current schema.

  - GLOBAL:x:

    Except DB IN ONE SERVER or DB IN MULTI SERVER,it routes a sql to a data node.Global table routes a update SQL to multiple dataNode and query SQL by loading balance.Consistency is required here.



- ##### data node

In MySQL connection,a dataNode is a meta data, a connection with a current schema that as session info.

That's why you can only use one schema to access MySQL server once with dataNode by a SQL without schema .

 

#### cconfiguration example

##### DB_IN_ONE_SERVER

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_ONE_SERVER
    defaultDataNode: dn1
    tables:
      - name: travelrecord


dataNodes:
  - name: dn1
    database: db1
    replica: repli
```

##### DB_IN_MULTI_SERVER

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_MULTI_SERVER
    tables:
      - name: travelrecord
        dataNodes: dn1
      - name: travelrecord2
        dataNodes: dn2


dataNodes:
  - name: dn1
    database: db1
    replica: repli
  - name: dn2
    database: db2
    replica: repli
```

##### ANNOTATION_ROUTE

```yaml
schemas:
  - name: db1
    schemaType: ANNOTATION_ROUTE
    tables:
      - name: travelrecord
        dataNodes: dn1,dn2,dn3,dn4
        type: SHARING_DATABASE
dataNodes:
  - name: dn1
    database: db1
    replica: repli
  - name: dn2
    database: db2
    replica: repli
  - name: dn3
    database: db3
    replica: repli
  - name: dn4
    database: db4
    replica: repli
```

## verification

### packet splitting

for example,client or server  send 2^24 -1 bytes payload int two packets. 

###  session status

checking pass status correctly from MySQL client to backend MySQL server.

```sql
SET autocommit = {1|0};
SET names {charset};
SET character_set_results  = {charset};
SET SESSION TRANSACTION ISOLATION LEVEL {isolation};//only support
USE {schema};
```

### hold the session

When front client has the following status,proxy should hold the backend client until end of the interaction process.

- transaction
- prepare statement(according to the specific implementation):x:
- loaddata infile:x:

## Proxy Test

Generally,for isolation complexity to test proxy separately.

### Packet parse

MySQL packet parse designed for proxy.For the row packet,column def packet,not need to receive the complete packet to pass the message data from the backend to the front.

#### A payload of request  in multi packet 

When the length of a SQL is exceeding 16MB,proxy must correctly reveive  it and send it to backend mysql server.Similarly,if proxy support send long (blob)  data command or load data infile,should test them.

so we prepare request as follow

- a long query sql statement
- a insert sql with blob prepare parameter to send long data
- load data file 

#### A  payload  response in multi packet 

When the length of a row packet of result set is exceeding 16MB,proxy must correctly reveive it from backend mysql server and maybe swap it to client during direct exchanging data in a net buffer .

so we prepare some data in backend mysql server as follow

- a row data exceeding 16MB

### Multi packet repsonse

#### COMMAND

a ok packet,error packet or eof packet

#### COM_FIELD_LIST 

a column count ,number of column def ,maybe  a eof packet

#### COM_QUERY|COM_STMT_EXECUTE 

1. if the number of row is 0,repsonse is only a ok packet.otherwise,
2. a result set contains a column count ,column count number of column def ,maybe a eof packet,multi row and end on ok packet or error packet.
3. if the last ok packet (head 0xfe),its serverstatus marked more result set,response continues on step 1 .

#### COM_STMT_PREPARE 

COM_STMT_PREPARE_OK,num_params  number of column def and num_columns number of column def 

#### LOCAL INFILE Request (from server send to client)

Client and server interact multiple times.



if the CLIENT_DEPRECATE_EOF in on ,eof packet will be disappeared,so eof packet should not be used as a basis for processing responses.Counting the number of column def  instead of it.

### Multi statement SQL(Multi text result set)

if a SQL contains multi statements separated by **;** ,its result set is multi.if it is not prepare statement,the result set is text result set.if one of the sql statements is not a query statement or the  row of result set  count is 0,its corresponding message is ok packet.

so we prepare request as follow

-  SQL cantains query statement and update statement.

### Multi binary result set

Said above the multi satement SQL,prepare statement execute return multi binary result set

so we prepare request as follow

- a query SQL cantains query multi statement and update statement. by prepare statement.

### Session status synchronization

Said above verification session status,these status in client and proxy session or backend session should be consistent with client,proxy session and backend mysql session.Because a backend mysql session can be held by different multi proxy sessions if it  could so that its status maybe not  consistent with client.

 Normally,before sending truly SQL,proxy session must get a usable session and send the update status statement SQL.

Interested in the following status:

- autocommit
- charset
- character_set_results(result set charset)
- transaction isolation
- default schema

### Test Contract

### tools

two MySQL servers(no proxy) with account with 

 user name {root} and 

password {123456}

one server  listens{localhost:3307}

the other  listens{localhost:3308} 

with a schema named {db1}

Generally,we test proxy by a table named travelrecord .

```sql
CREATE TABLE `travelrecord` (
  `id` bigint(20) NOT NULL,
  `user_id` varchar(100) DEFAULT NULL,
  `traveldate` date DEFAULT NULL,
  `fee` decimal(10,0) DEFAULT NULL,
  `days` int(11) DEFAULT NULL
) 
```

when we need more schema or table,

we rename db1,db2,db3... 

travelrecord1,travelrecord2,travelrecord3...



maybe set mysql config

```sql
SET GLOBAL time_zone='+8:00';//maybe set time zone
SET GLOBAL max_connections= 20000;
SET GLOBAL max_allowed_packet = 2*10*1024*1024;//to test multi packet
```





### mycat proxy configuration 

the test cares mysql proxy instead of strategy which a function has nothing to do with network data

```yaml
#replicas.yaml
replicas:
  - name: repli                   # 
    repType: MASTER_SLAVE         # do not care
    switchType: SWITCH            # do not care
    balanceName: BalanceAllRead   # do not care
    mysqls:
      - name: mytest3306             
        ip: 127.0.0.1               #
        port: 3306                  # 
        user: root                  # 
        password: 123456     		# 
        minCon: 1                   # do not care
        maxCon: 1000                # do not care
        maxRetryCount: 3            # do not care
      - name: mytest3307            
        ip: 127.0.0.1               # 
        port: 3307                  # 
        user: root                  # 
        password: 123456     		# 
        minCon: 1                   # do not care
        maxCon: 1000                # do not care
        maxRetryCount: 3            # do not care
```

```yaml
#schema.yml
schemas:
  - name: test
    schemaType: DB_IN_ONE_SERVER
    defaultDataNode: dn1
    tables:
      - name: travelrecord
dataNodes:
  - name: dn1
    database: db1
    replica: repli
```

The above is the simplest configuration to test the proxy about router

```yaml
#users.yaml
users:
  - name: root
    password: 123456
    schemas:
      - test
```

```yaml
#mycat.yaml
proxy:
  ip: 0.0.0.0
  port: 8066
  bufferPoolPageSize: 4194304     # do not care
  bufferPoolChunkSize: 8192       # default
  bufferPoolPageNumber: 2        # default
  reactorNumber: 6        # default
```

### exception 

1. #### reveive data exception from client

   close backend mysql session if it exists

   close mycat session

2. #### write data from proxy to backend mysql server  exception 

   ##### option 1 send a error packet(default):

   close backend mysql session

   send a error packet to client

   ##### option 2 retry:

   set a counter 

   close backend mysql session

   get a new backend mysql session

   until write succeessfully or datasource is died or other excpetion

   ##### option 3 close:

   close mycat session(client reveice)

   close mycat session

3. #### reveive data from backend mysql server exception 

   close backend mysql session

   close mycat session

4. #### write data to client exception 

   close backend mysql session

   close mycat session



### Platform validation

mysql client (most jdbc) 

mysql server 5.5/5.6/5.7/8



### Testing process(temporary)

1. startup mycat 

2. client connects mycat and get a connection 

3. client sends a SQL whose length is  exceeding 16MB

4. client sends a SQL whitch querys a result set contains a row  exceeding 16MB

5. client sends multi statements in a SQL contains query statement and update statement

6.  client sends multi statements in a SQL contains query statement and update statement by preparement

7.  client sends begin statement,mycat can not unbind the backend mysql session

8.  client sends any statement,mycat can not unbind the backend mysql session until commit or rollback
9.  client sends loadata infile ,mycat can not unbind the backend mysql session until loaddata infile completed:x:
10.  client sends query statement with cursor,mycat can not unbind the backend mysql session until close cursor:x:
11.  client sends multi result store procedure without parameters (because not support   preparestatement with parameters   yet):x:

12.  mycat as client sends load data infile:x:



## package

mycat2 module

```bash
mvn package
```



## run/debug

path to the configuration file(resources) as MYCAT_HOME added to VM options.

```
java -Dfile.encoding=UTF-8 -DMYCAT_HOME=D:\xxxxxxx -jar mycat2-0.1.jar 
```

## communicate

leave a message on issues.

Maintain together to make the project more robust.

QQ:294712221

## License

GPLv3
