# 前言

![](./mysql_connect.png)

由前面的文章我们已经分析了mycat链接建立的前7步,现在我们接着mycat分析之五-mycat前端链接建立第七步 来分析.

# 分析

1. 当mycat把查询版本信息的包发送给mysql端后,mysql会将查询的数据发送给mycat.至此,又进入了ProxyReactorThread轮训事件,交由相应的handler进行处理.因此会调用io.mycat.mycat2.net.DefaultMycatSessionHandler#onSocketRead方法.

2. 在io.mycat.mycat2.net.DefaultMycatSessionHandler#onSocketRead中处理逻辑如下:
	1. 从session中获得MySQLCommand.
	2. 调用io.mycat.mycat2.MySQLCommand#onBackendResponse进行处理.处理成功后,调用io.mycat.mycat2.MySQLCommand#clearBackendResouces方法.如果在处理过程中,出现异常,则关闭后端连接,同时向前端返回错误信息.
代码如下:

	```
	private void onBackendRead(MySQLSession session) throws IOException {
			// 交给SQLComand去处理
			MySQLCommand curCmd = session.getMycatSession().curSQLCommand;
			try{
				if (curCmd.onBackendResponse(session)) {
					curCmd.clearBackendResouces(session,session.isClosed());
				}
			}catch(ClosedChannelException  ex){
				String errmsg =  " read backend response error ,backend conn has closed.";
				logger.error(errmsg);
				session.getMycatSession().closeBackendAndResponseError(session,false,ErrorCode.ERR_CONNECT_SOCKET,errmsg);
			} catch (IOException e) {
				logger.error(" read backend response error.",e);
				session.getMycatSession().closeBackendAndResponseError(session,false,ErrorCode.ERR_CONNECT_SOCKET, e.getMessage());
			}
		}
	```

2.2.1 在io.mycat.mycat2.cmds.DirectPassthrouhCmd#onBackendResponse中,处理过程如下:
	
1. 调用io.mycat.proxy.AbstractSession#readFromChannel,进行一次报文的读取操作.该方法我们之前已经分析过了.
2. 进行报文的处理流程.调用io.mycat.mycat2.cmds.pkgread.PkgProcess#procssPkg进行处理.
3. 获取当前是否结束标识.并根据该标识做不同的处理.如果为true,则意味着当前操作已经完成,注册读事件.否则,注册写操作,直接透传报文.
4. 调用io.mycat.proxy.AbstractSession#writeToChannel,写回给客户端.

代码如下:

```
public boolean onBackendResponse(MySQLSession session) throws IOException {

		// 首先进行一次报文的读取操作
		if (!session.readFromChannel()) {
			return false;
		}

		// 进行报文处理的流程化
		boolean nextReadFlag = false;
		do {
			// 进行报文的处理流程
			nextReadFlag = session.currPkgProc.procssPkg(session);
		} while (nextReadFlag);

		// 获取当前是否结束标识
		Boolean check = (Boolean) session.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

		MycatSession mycatSession = session.getMycatSession();
		ProxyBuffer buffer = session.getProxyBuffer();

		buffer.flip();
		// 检查到当前已经完成,执行添加操作
		if (null != check && check) {
			// 当知道操作完成后，前段的注册感兴趣事件为读取
			mycatSession.takeOwner(SelectionKey.OP_READ);
		}
		// 未完成执行继续读取操作
		else {
			// 直接透传报文
			mycatSession.takeOwner(SelectionKey.OP_WRITE);
		}

		mycatSession.writeToChannel();

		return false;
	}
```
2.2.1.2 在io.mycat.mycat2.cmds.pkgread.PkgFirstReader#procssPkg方法中处理的逻辑如下:
	
1.调用io.mycat.mycat2.AbstractMySQLSession#resolveMySQLPackage进行首次的报文解析.
2. 如果报文解析完毕,则进行处理,否则,继续读取.
 > note:继续读取的逻辑是通过在io.mycat.mycat2.cmds.DirectPassthrouhCmd#onBackendResponse中的一个死循环完成的.代码如下:
 
 ```
 boolean nextReadFlag = false;
		do {
			// 进行报文的处理流程
			nextReadFlag = session.currPkgProc.procssPkg(session);
		} while (nextReadFlag);

 ```
 
 3. 如果extendCmdPkg包含着当前执行的命令,返回false.
 4. 如果当前为查询包，则切换到查询的逻辑命令处理
 5. 如果当前为特殊的load data包，则直接进行切换至load data的逻辑处理
 6. 如果为ok和error则切换到error的包判断
 7. 对于首包非完整的，透传已经检查完毕
 
 由于当前是一个查询包,因此会进入第4步:
	*  	标识当前为查询
	*  	将currPkgProc切换至查询的读取操作
	*  	返回true.
 代码如下:
 
 ```
  if (QUERY_PKG_START <= pkgType) {

				// 标识当前为查询
				session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_PKG_TYPE_KEY.getKey(),
						MySQLPacket.RESULTSET_PACKET);

				// 当前确认查询包，则切换至查询的读取操作
				session.currPkgProc = PkgResultSetReader.INSTANCE;
				return true;
			}
 ```
  
2.2.1.3 由于当前SESSION_KEY_CONN_IDLE_FLAG为false,因此会调用io.mycat.mycat2.MycatSession#takeOwner,注册写事件,并且由于当前的curBackend(后端链接)不为空,因此会调用io.mycat.mycat2.AbstractMySQLSession#setCurBufOwner和clearReadWriteOpts方法.代码如下:

```
	public void takeOwner(int intestOpts) {
		this.curBufOwner = true;
		if (intestOpts == SelectionKey.OP_READ) {
			this.change2ReadOpts();
		} else {
			this.change2WriteOpts();
		}
		if (this.curBackend != null) {
			curBackend.setCurBufOwner(false);
			curBackend.clearReadWriteOpts();
		}
	}
```

# 总结
至此我们就分析完mycat前端建立链接的过程,后续会围绕着sql语句的处理,分布式事务的实现进行分析,敬请期待.

