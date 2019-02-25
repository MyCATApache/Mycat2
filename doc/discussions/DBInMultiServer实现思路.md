| version | date | participants | content |
|:---------:|------|:--------------:|:------------:|
| 1.0     | 2017-12-29 | 鼯鼠|新建文档|
| 1.1     | 2018-01-20 | 鼯鼠|支持全局表|

### 1. DBInMultiServer目标

* 支持DBInMultiServer模式，即表在不同的MySQL Server上，但不分片
* 暂不允许跨节点DML ，DDL语句（给出错误提示）（已支持）
* 兼容MyCAT 动态注解，静态注解。（待测试）
* 支持全局表

### 2.  实现思路

#### 2.1 配置


* schema.yml

```
schemas:
  - name: myfly
    schemaType: DB_IN_MULTI_SERVER
    defaultDataNode: dn1
    tables:
      - name: tb_fly
        dataNode: dn1
      - name: tb_boy
        dataNode: dn2
      - name: tb_paw
        type: global
        dataNode: dn$1-2
dataNodes:
  - name: dn1
    database: mytest
    replica: repli
  - name: dn2
    database: mytest2
```


#### 2.2 路由实现

* 定义一个RouteStratagy接口和一个针对DBInMultiServer模式的实现类DBInMultiServerRouteStrategy。

该实现类的路由逻辑大致如下：

根据sql解析出涉及到哪些表，根据配置找出表所在的datanode节点。
	

* 在AbstractCmdStrategy中定义一个抽象方法如delegateRoute,把这个方法加入到AbstractCmdStrategy类的matchMySqlCommand方法中。

融入delegateRoute的matchMySqlCommand大致如下：

```
@Override
    final public boolean matchMySqlCommand(MycatSession session) {
		
		...		
		parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
		...
		SQLAnnotationChain chain = new SQLAnnotationChain();
		session.curSQLCommand = chain.setTarget(command) 
			 .processDynamicAnno(session)
			 .processStaticAnno(session, staticAnnontationMap)
			 .build();
        // 处理路由
        if (!delegateRoute(session)) {
            return false;
        }
		return true;
	}
```

* AbstractCmdStrategy的子类实现delegateRoute

delegateRoute调用RouteStratagy获取路由节点（datanode），如果跨节点则返回错误给mysql客户端，不跨节点则执行记录路由信息到到当前MyCatSession。如下


```
 @Override
    protected boolean delegateRoute(MycatSession session) {

        byte sqltype = session.sqlContext.getSQLType() != 0 ? session.sqlContext.getSQLType()
                : session.sqlContext.getCurSQLType();
        RouteResultset rrs = routeStrategy.route(session.schema, sqltype,
                session.sqlContext.getRealSQL(0), null, session);
        if (rrs.getNodes() != null && rrs.getNodes().length > 1) {
            session.curRouteResultset = null;
            try {
                logger.error(
                        "Multi node error! Not allowed to execute SQL statement across data nodes in DB_IN_MULTI_SERVER schemaType.\n"
                                + "Original SQL:[{}]",
                        session.sqlContext.getRealSQL(0));
                session.sendErrorMsg(ErrorCode.ERR_MULTI_NODE_FAILED,
                        "Not allowed to execute SQL statement across data nodes in DB_IN_MULTI_SERVER schemaType.");
            } catch (IOException e) {
                session.close(false, e.getMessage());
            }
            return false;
        } else {
            session.curRouteResultset = rrs;
        }
        return true;
    }

```

* 改造MycatSesssion中获取后端连接的方法 getbackendName()

```
private String getbackendName(){
		...
			case DB_IN_MULTI_SERVER:
                RouteResultsetNode[] nodes = this.curRouteResultset.getNodes();
                if (nodes != null && nodes.length > 0) {
                    String dataNodeName = nodes[0].getName();
                    DNBean dnBean = ProxyRuntime.INSTANCE.getConfig().getDNBean(dataNodeName);
                    if (dnBean != null) {
                        backendName = dnBean.getReplica();
                    }
                } else {
                    backendName = schema.getDefaultDN().getReplica();
                }
				break;
//			case SQL_PARSE_ROUTE:
//				break;
			default:
				break;
		}
		if (backendName == null){
			throw new InvalidParameterException("the backendName must not be null");
		}
		return backendName;
	}
```

#### 2.3 全局表

尽管DBInMultiServer模式一般情况下不允许跨库DML ，DDL语句，但全局表是个例外，仅针对全局表的DDL，DML语句可以跨节点执行。

全局表具有如下特性：

* 全局表的插入、更新操作会实时在所有节点上执行，保持各个分片的数据一致性
* 全局表的查询操作，只从一个节点获取
* 全局表可以跟任何一个表进行JOIN 操作

### 3.讨论点
#### 3.1 数据库管理语句支持到什么程度

如下这些DAS

```
13.7 Database Administration Statements ............................................................................. 194013.7.1 Account Management Statements ....................................................................... 194013.7.2 Table Maintenance Statements ........................................................................... 197113.7.3 Plugin and User-Defined Function Statements ..................................................... 198113.7.4 SET Syntax ....................................................................................................... 198413.7.5 SHOW Syntax ................................................................................................... 198713.7.6 Other Administrative Statements
```

常用的如show 语句，create user 等。

DAS 种类繁多，MyCat1.6 只考虑了 show tables ,select @@version,desc .如下

```

public RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType,
			String stmt, RouteResultset rrs) throws SQLSyntaxErrorException {
		switch(sqlType){
		case ServerParse.SHOW:// if origSQL is like show tables
			return analyseShowSQL(schema, rrs, stmt);
		case ServerParse.SELECT://if origSQL is like select @@
			int index = stmt.indexOf("@@");
			if(index > 0 && "SELECT".equals(stmt.substring(0, index).trim().toUpperCase())){
				return analyseDoubleAtSgin(schema, rrs, stmt);
			}
			break;
		case ServerParse.DESCRIBE:// if origSQL is meta SQL, such as describe table
			int ind = stmt.indexOf(' ');
			stmt = stmt.trim();
			return analyseDescrSQL(schema, rrs, stmt, ind + 1);
		}
		return null;
	}
```

MyCat2.0 要支持到什么程度呢？