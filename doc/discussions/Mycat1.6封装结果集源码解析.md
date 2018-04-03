### Mycat1.6封装结果集源码解析

王亚飞

#### 1.Mycat执行普通sql语句

1. 设置sql的隔离级别
2. sql是否是存储过程并且数据库引擎是oracle 如果是执行 存储过程
3. 判断sql执行类型,并根据sql、数据库引擎、特殊sql语句选择相应的方法执行sql


    private void executeSQL(RouteResultsetNode rrn, ServerConnection sc,
    						boolean autocommit) throws IOException {
    	//获取sql语句
    	String orgin = rrn.getStatement();
    	// String sql = rrn.getStatement().toLowerCase();
    	// LOGGER.info("JDBC SQL:"+orgin+"|"+sc.toString());
    	//是否能走从库
    	if (!modifiedSQLExecuted && rrn.isModifySQL()) {
    		modifiedSQLExecuted = true;
    	}
    	try {
    	    //设置数据库隔离级别
            syncIsolation(sc.getTxIsolation()) ;
    		if (!this.schema.equals(this.oldSchema)) {
    			con.setCatalog(schema);
    			this.oldSchema = schema;
    		}
    		if (!this.isSpark) {
    			con.setAutoCommit(autocommit);
    		}
    		//获取数据库sql类型SELECT/SHOW/INSERT/UPDATE/DELETE语句
    		int sqlType = rrn.getSqlType();
    		//判断sql是否是存储语句并且引擎是oracle
             if(rrn.isCallStatement()&&"oracle".equalsIgnoreCase(getDbType()))
             {
                 //存储过程暂时只支持oracle
                 ouputCallStatement(rrn,sc,orgin);
             }  
             //如果是SELETE或者SHOW语句
             else if (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW) {
                //如果是SHOW语句并且数据库引擎是mysql
    			if ((sqlType == ServerParse.SHOW) && (!dbType.equals("MYSQL"))) {
    				// showCMD(sc, orgin);
    				//ShowVariables.execute(sc, orgin);
    				ShowVariables.execute(sc, orgin,this);
    			} else if ("SELECT CONNECTION_ID()".equalsIgnoreCase(orgin)) {//如果是特殊语句
    				//ShowVariables.justReturnValue(sc,String.valueOf(sc.getId()));
    				ShowVariables.justReturnValue(sc,String.valueOf(sc.getId()),this);
    			} else {
    				ouputResultSet(sc, orgin);
    			}
    		} else {//如果是INSERT/UPDATE/DELETE语句
    			executeddl(sc, orgin);
    		}
    
    	} catch (SQLException e) {//如果报错
    
    		String msg = e.getMessage();
    		ErrorPacket error = new ErrorPacket();
    		error.packetId = ++packetId;
    		error.errno = e.getErrorCode();
    		error.message = msg.getBytes();
    		this.respHandler.errorResponse(error.writeToBytes(sc), this);
    	}catch (Exception e) {//如果报错
    		String msg = e.getMessage();
    		ErrorPacket error = new ErrorPacket();
    		error.packetId = ++packetId;
    		error.errno = ErrorCode.ER_UNKNOWN_ERROR;
    		error.message = ((msg == null) ? e.toString().getBytes() : msg.getBytes());
    		String err = null;
    		if(error.message!=null){
    		    err = new String(error.message);
    		}
    		LOGGER.error("sql execute error, "+ err , e);
    		this.respHandler.errorResponse(error.writeToBytes(sc), this);
    	}
    	finally {
    		this.running = false;
    	}
    }

#### Mycat解析sql结果

##### 执行sql并将执行返回的头、元数据、尾、数据等添加到对应的解析结果的byteBuf中

    private void ouputResultSet(ServerConnection sc, String sql)
            throws SQLException {
        ResultSet rs = null;
        Statement stmt = null;
    
    	try {
    		//获取连接
    		stmt = con.createStatement();
    		//执行sql
    		rs = stmt.executeQuery(sql);
    		//初始化list 用来存储sql执行结果元数据
    		List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
    		//添加sql执行结果元数据
    		ResultSetUtil.resultSetToFieldPacket(sc.getCharset(), fieldPks, rs,
    				this.isSpark);
    		//获取list大小即查询出数据的条数
    		int colunmCount = fieldPks.size();
    		//创建一个bytebuffer
    		ByteBuffer byteBuf = sc.allocate();
    		//头信息
    		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
    		headerPkg.fieldCount = fieldPks.size();
    		headerPkg.packetId = ++packetId;
    		//将headerPkg数据写到bytebuf中
    		byteBuf = headerPkg.write(byteBuf, sc, true);
    		//刷新(切换读写模式)
    		byteBuf.flip();
    		//创建一个byte数组
    		byte[] header = new byte[byteBuf.limit()];
    		//将bytebuf中的数据刷新到header数组中
    		byteBuf.get(header);
    		//清空bytebuf
    		byteBuf.clear();
    		//初始化list
    		List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
    		//迭代fieldPks
    		Iterator<FieldPacket> itor = fieldPks.iterator();
    		//将结果信息放到fields集合中
    		while (itor.hasNext()) {
    			//获取FieldPacket数据
    			FieldPacket curField = itor.next();
    			//序号
    			curField.packetId = ++packetId;
    			//将FieldPacket信息写入bytebuf中
    			byteBuf = curField.write(byteBuf, sc, false);
    			//切换读写模式
    			byteBuf.flip();
    			//创建byte数组
    			byte[] field = new byte[byteBuf.limit()];
    			//将bytebuf中的数据刷新到byte数组中
    			byteBuf.get(field);
    			//清空byteBuf
    			byteBuf.clear();
    			//将field数据添加到集合中
    			fields.add(field);
    		}
    		//添加数据尾数据
    		EOFPacket eofPckg = new EOFPacket();
    		//设置packetId
    		eofPckg.packetId = ++packetId;
    		//将数据刷新到byteBuf中
    		byteBuf = eofPckg.write(byteBuf, sc, false);
    		//切换读写模式
    		byteBuf.flip();
    		//创建数组
    		byte[] eof = new byte[byteBuf.limit()];
    		//将数据写到数组中
    		byteBuf.get(eof);
    		//清空byteBuf
    		byteBuf.clear();
    		//根据数据集的类型(respHandler的子类) 将header、fields、eof数据写到byteBuf中去
    		this.respHandler.fieldEofResponse(header, fields, eof, this);
    
    		// output row
    		//解析数据(将结果添加到待解析buf中)
    		while (rs.next()) {
    			//创建RowDataPacket(行数据信息)对象
    			RowDataPacket curRow = new RowDataPacket(colunmCount);
    			for (int i = 0; i < colunmCount; i++) {
    				int j = i + 1;
    				if(MysqlDefs.isBianry((byte) fieldPks.get(i).type)) {
    						curRow.add(rs.getBytes(j));
    				} else if(fieldPks.get(i).type == MysqlDefs.FIELD_TYPE_DECIMAL ||
    						fieldPks.get(i).type == (MysqlDefs.FIELD_TYPE_NEW_DECIMAL - 256)) { // field type is unsigned byte
    					// ensure that do not use scientific notation format
    					BigDecimal val = rs.getBigDecimal(j);
    					curRow.add(StringUtil.encode(val != null ? val.toPlainString() : null,
    							sc.getCharset()));
    				} else {
    					   curRow.add(StringUtil.encode(rs.getString(j),
    							   sc.getCharset()));
    				}
    
    			}
    			curRow.packetId = ++packetId;
    			byteBuf = curRow.write(byteBuf, sc, false);
    			byteBuf.flip();
    			byte[] row = new byte[byteBuf.limit()];
    			byteBuf.get(row);
    			byteBuf.clear();
    			this.respHandler.rowResponse(row, this);
    		}
    
    		fieldPks.clear();
    
    		// end row
    		//添加尾数据
    		eofPckg = new EOFPacket();
    		eofPckg.packetId = ++packetId;
    		byteBuf = eofPckg.write(byteBuf, sc, false);
    		byteBuf.flip();
    		eof = new byte[byteBuf.limit()];
    		byteBuf.get(eof);
    		sc.recycle(byteBuf);
    		this.respHandler.rowEofResponse(eof, this);
    	} finally {
    		if (rs != null) {
    			try {
    				rs.close();
    			} catch (SQLException e) {
    
    			}
    		}
    		if (stmt != null) {
    			try {
    				stmt.close();
    			} catch (SQLException e) {
    
    			}
    		}
    	}
    }
    
    
    
    
    

##### 单节点拼接结果集(SingleNodeHandler)
##### 单节点拼接结果集(SingleNodeHandler)

###### SELECT

1. fieldEofResponse()：元数据返回时触发，将header和元数据内容依次写入缓冲区中；


    public void fieldEofResponse(byte[] header, List<byte[]> fields,
    		byte[] eof, BackendConnection conn) {
    	this.header = header;
    	this.fields = fields;
    	MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
        if(null !=middlerResultHandler ){
    		return;
    	}
    	this.netOutBytes += header.length;
    	for (int i = 0, len = fields.size(); i < len; ++i) {
    		byte[] field = fields.get(i);
    		this.netOutBytes += field.length;
    	}
    
    	header[3] = ++packetId;
    	ServerConnection source = session.getSource();
    	buffer = source.writeToBuffer(header, allocBuffer());
    	for (int i = 0, len = fields.size(); i < len; ++i) {
    		byte[] field = fields.get(i);
    		field[3] = ++packetId;
    		
    		 // 保存field信息
 			FieldPacket fieldPk = new FieldPacket();
 			fieldPk.read(field);
 			fieldPackets.add(fieldPk);
			
			buffer = source.writeToBuffer(field, buffer);
		}
		
		fieldCount = fieldPackets.size();
		
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, buffer);
	
		if (isDefaultNodeShowTable) {
			
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
			
		} else if (isDefaultNodeShowFullTable) {
			
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.add(StringUtil.encode("BASE TABLE", source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
		}
	}
2. rowResponse()：行数据返回时触发，将行数据写入缓冲区中；


    public void rowResponse(byte[] row, BackendConnection conn) {
    	
    	this.netOutBytes += row.length;
    	this.selectRows++;
    	
    	if (isDefaultNodeShowTable || isDefaultNodeShowFullTable) {
    		RowDataPacket rowDataPacket = new RowDataPacket(1);
    		rowDataPacket.read(row);
    		String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), session.getSource().getCharset());
    		if (shardingTablesSet.contains(table.toUpperCase())) {
    			return;
    		}
    	}
    	row[3] = ++packetId;
    	
    	if ( prepared ) {			
    		RowDataPacket rowDataPk = new RowDataPacket(fieldCount);
    		rowDataPk.read(row);			
    		BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
    		binRowDataPk.read(fieldPackets, rowDataPk);
    		binRowDataPk.packetId = rowDataPk.packetId;
            //binRowDataPk.write(session.getSource());
    		/*
    		 * [fix bug] : 这里不能直接将包写到前端连接,
    		 * 因为在fieldEofResponse()方法结束后buffer还没写出,
    		 * 所以这里应该将包数据顺序写入buffer(如果buffer满了就写出),然后再将buffer写出
    		 */
    		buffer = binRowDataPk.write(buffer, session.getSource(), true);
    	} else {
    
    		MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
            if(null ==middlerResultHandler ){
            	 buffer = session.getSource().writeToBuffer(row, allocBuffer());
    		}else{
    	        if(middlerResultHandler instanceof MiddlerQueryResultHandler){
    	        	byte[] rv = ResultSetUtil.getColumnVal(row, fields, 0);
    				 	 String rowValue =  rv==null?"":new String(rv);
    					 middlerResultHandler.add(rowValue);	
 				 }
			}
		 
		}
	
	}
	

3. rowEofResponse()：行结束标志返回时触发，将EOF标志写入缓冲区，最后调用source.write(buffer)将缓冲区放入前端连接的写缓冲队列中，等待NIOSocketWR将其发送给应用。
3. rowEofResponse()：行结束标志返回时触发，将EOF标志写入缓冲区，最后调用source.write(buffer)将缓冲区放入前端连接的写缓冲队列中，等待NIOSocketWR将其发送给应用。


    public void rowEofResponse(byte[] eof, BackendConnection conn) {
    	
    	this.netOutBytes += eof.length;
    	
    	ServerConnection source = session.getSource();
    	conn.recordSql(source.getHost(), source.getSchema(), node.getStatement());
        // 判断是调用存储过程的话不能在这里释放链接
    	if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue())) 
    	{
    		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
    		endRunning();
    	}
    
    	eof[3] = ++packetId;
    	buffer = source.writeToBuffer(eof, allocBuffer());
    	int resultSize = source.getWriteQueue().size()*MycatServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
    	resultSize=resultSize+buffer.position();
    	MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
    
    	if(middlerResultHandler !=null ){
    		middlerResultHandler.secondEexcute(); 
    	} else{
    		source.write(buffer);
    	}
    	source.setExecuteSql(null);
    	//TODO: add by zhuam
    	//查询结果派发
    	QueryResult queryResult = new QueryResult(session.getSource().getUser(), 
    			rrs.getSqlType(), rrs.getStatement(), affectedRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),resultSize);
    	QueryResultDispatcher.dispatchQuery( queryResult );
    	
    }

###### UPDATE/INSERT/DELETE
调用链路:JDBCConnection.executeSQL->JDBCConnection.executeddl->SingleNodeHandler.okResponse


    private void executeddl(ServerConnection sc, String sql)
    		throws SQLException {
    	Statement stmt = null;
    	try {
    		stmt = con.createStatement();
    		int count = stmt.executeUpdate(sql);
    		OkPacket okPck = new OkPacket();
    		okPck.affectedRows = count;
    		okPck.insertId = 0;
    		okPck.packetId = ++packetId;
    		okPck.message = " OK!".getBytes();
    		this.respHandler.okResponse(okPck.writeToBytes(sc), this);
    	} finally {
    		if (stmt != null) {
    			try {
    				stmt.close();
    			} catch (SQLException e) {
    
    			}
    		}
    	}
    }
    
    
    public void okResponse(byte[] data, BackendConnection conn) {      
    	//
    	this.netOutBytes += data.length;
    	
    	boolean executeResponse = conn.syncAndExcute();		
    	if (executeResponse) {
    		ServerConnection source = session.getSource();
    		OkPacket ok = new OkPacket();
    		ok.read(data);
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());
    		if (rrs.isLoadData()) {				
    			byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
    			ok.packetId = ++lastPackId;// OK_PACKET
    			source.getLoadDataInfileHandler().clear();
    			
    		} else if (isCanClose2Client) {
    			ok.packetId = ++packetId;// OK_PACKET
    		}
    
    
    		if (isCanClose2Client) {
    			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
    			endRunning();
    		}
    		ok.serverStatus = source.isAutocommit() ? 2 : 1;
    		recycleResources();
    
    		if (isCanClose2Client) {
    			source.setLastInsertId(ok.insertId);
    			ok.write(source);
    		}
            
    		this.affectedRows = ok.affectedRows;
    		
    		source.setExecuteSql(null);
    		// add by lian
    		// 解决sql统计中写操作永远为0
    		QueryResult queryResult = new QueryResult(session.getSource().getUser(), 
    				rrs.getSqlType(), rrs.getStatement(), affectedRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),0);
    		QueryResultDispatcher.dispatchQuery( queryResult );
    	}
    }
    

##### 多节点拼接结果集(MultiNodeQueryHandler)
##### 多节点拼接结果集(MultiNodeQueryHandler)

1. 获取是否启用Off Heap(默认启用)isOffHeapuseOffHeapForMerge
2. 根据是否是SELECT语句和是否需要合并和isOffHeapuseOffHeapForMerge获取dataMergeSvr


    public MultiNodeQueryHandler(int sqlType, RouteResultset rrs,
    		boolean autocommit, NonBlockingSession session) {
    	
    	super(session);
 		this.isMiddleResultDone = new AtomicBoolean(false);

		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("execute mutinode query " + rrs.getStatement());
		}
		
		this.rrs = rrs;
		//获取是否启用Off Heap
		isOffHeapuseOffHeapForMerge = MycatServer.getInstance().
				getConfig().getSystem().getUseOffHeapForMerge();
		//获取dataMergeSvr
		if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
			/**
			 * 使用Off Heap
			 */
			if(isOffHeapuseOffHeapForMerge == 1){
				dataMergeSvr = new DataNodeMergeManager(this,rrs,isMiddleResultDone);
			}else {
				dataMergeSvr = new DataMergeService(this,rrs);
			}
		} else {
			dataMergeSvr = null;
		}
		
		isCallProcedure = rrs.isCallStatement();
		this.autocommit = session.getSource().isAutocommit();
		this.session = session;
		this.lock = new ReentrantLock();
		// this.icHandler = new CommitNodeHandler(session);
	
		this.limitStart = rrs.getLimitStart();
		this.limitSize = rrs.getLimitSize();
		this.end = limitStart + rrs.getLimitSize();
	
		if (this.limitStart < 0)
			this.limitStart = 0;
	
		if (rrs.getLimitSize() < 0)
			end = Integer.MAX_VALUE;
		if ((dataMergeSvr != null)
				&& LOGGER.isDebugEnabled()) {
				LOGGER.debug("has data merge logic ");
		}
		
		if ( rrs != null && rrs.getStatement() != null) {
			netInBytes += rrs.getStatement().getBytes().length;
		}
	}

3. 初始化grouper和sorter

调用链路：JDBCConnection.ouputResultSet ->MultiNodeQueryHandler.fieldEofResponse
->dataMergeSvr.onRowMetaData

4. 将数据放入List<FieldPacket>

调用链路:JDBCConnection.ouputResultSet ->
dataMergeSvr.onNewRecord


    public boolean onNewRecord(String dataNode, byte[] rowData) {
        final PackWraper data = new PackWraper();
        data.dataNode = dataNode;
        data.rowData = rowData;
        addPack(data);
        return false;
    }
5. 获取数据生成RowDataPacket实例


    public void run() {
    	if(!running.compareAndSet(false, true)){
    		return;
    	}
    	boolean nulpack = false;
    	try{
    		for (; ; ) {
    			final PackWraper pack = packs.poll();
    			if(pack == null){
    				nulpack = true;
    				break;
    			}
    			if (pack == END_FLAG_PACK) {
    				final int warningCount = 0;
    				final EOFPacket eofp   = new EOFPacket();
    				final ByteBuffer eof   = ByteBuffer.allocate(9);
    				BufferUtil.writeUB3(eof, eofp.calcPacketSize());
    				eof.put(eofp.packetId);
    				eof.put(eofp.fieldCount);
    				BufferUtil.writeUB2(eof, warningCount);
    				BufferUtil.writeUB2(eof, eofp.status);
    				final ServerConnection source = multiQueryHandler.getSession().getSource();
    				final byte[] array = eof.array();
    				break;
    			}
    			final RowDataPacket row = new RowDataPacket(fieldCount);
    			row.read(pack.rowData);
    			if (grouper != null) {
    				grouper.addRow(row);
    			} else if (sorter != null) {
    				if (!sorter.addRow(row)) {
    					canDiscard.put(pack.dataNode,true);
    				}
    			} else {
    				result.get(pack.dataNode).add(row);
    			}
    		}// rof
    	}catch(final Exception e){
    		multiQueryHandler.handleDataProcessException(e);
    	}finally{
    		running.set(false);
    	}
    	if(nulpack && !packs.isEmpty()){
    		this.run();
    	}
    }

6. 调用multiQueryHandler.outputMergeResult将结果发送到前端
6. 调用multiQueryHandler.outputMergeResult将结果发送到前端


    public void outputMergeResult(final ServerConnection source,
    		final byte[] eof, List<RowDataPacket> results) {
    	try {
    		lock.lock();
    		ByteBuffer buffer = session.getSource().allocate();
    		final RouteResultset rrs = this.dataMergeSvr.getRrs();
    
    		// 处理limit语句
    		int start = rrs.getLimitStart();
    		int end = start + rrs.getLimitSize();
    
    		if (start < 0) {
    			start = 0;
    		}
    		if (rrs.getLimitSize() < 0) {
    			end = results.size();
    		}
    		if (end > results.size()) {
    			end = results.size();
    		}
    		if(prepared) {
    			for (int i = start; i < end; i++) {
    				RowDataPacket row = results.get(i);
    				BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
    				binRowDataPk.read(fieldPackets, row);
    				binRowDataPk.packetId = ++packetId;
    				buffer = binRowDataPk.write(buffer, session.getSource(), true);
    			}
    		} else {
    			for (int i = start; i < end; i++) {
    				RowDataPacket row = results.get(i);
    				row.packetId = ++packetId;
    				buffer = row.write(buffer, source, true);
    			}
    		}
    		eof[3] = ++packetId;
    		if (LOGGER.isDebugEnabled()) {
    			LOGGER.debug("last packet id:" + packetId);
    		}
    		source.write(source.writeToBuffer(eof, buffer));
    
    	} catch (Exception e) {
    		handleDataProcessException(e);
    	} finally {
    		lock.unlock();
    		dataMergeSvr.clear();
    	}
    }

###### UPDATE/INSERT/DELETE
调用链路:JDBCConnection.executeSQL->JDBCConnection.executeddl->MultiNodeQueryHandler.okResponse

这三类语句都会返回一个OK包，里面包含了最为核心的affectedRows，因此每得到一个MySQL节点发送回的affectedRows，就将其累加，当收到最后一个节点的数据后(通过decrementOkCountBy()方法判断)，将结果返回给前端



    	public void okResponse(byte[] data, BackendConnection conn) {
    	this.netOutBytes += data.length;
    	
    	boolean executeResponse = conn.syncAndExcute();
    	if (LOGGER.isDebugEnabled()) {
    		LOGGER.debug("received ok response ,executeResponse:"
    				+ executeResponse + " from " + conn);
    	}
    	if (executeResponse) {
    
    		ServerConnection source = session.getSource();
    		OkPacket ok = new OkPacket();
    		ok.read(data);
            //存储过程
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());;
             if(!isCallProcedure)
             {
                 if (clearIfSessionClosed(session))
                 {
                     return;
                 } else if (canClose(conn, false))
                 {
                     return;
                 }
             }
    		lock.lock();
    		try {
    			// 判断是否是全局表，如果是，执行行数不做累加，以最后一次执行的为准。
    			if (!rrs.isGlobalTable()) {
    				affectedRows += ok.affectedRows;
    			} else {
    				affectedRows = ok.affectedRows;
    			}
    			if (ok.insertId > 0) {
    				insertId = (insertId == 0) ? ok.insertId : Math.min(
    						insertId, ok.insertId);
    			}
    		} finally {
    			lock.unlock();
    		}
    		// 对于存储过程，其比较特殊，查询结果返回EndRow报文以后，还会再返回一个OK报文，才算结束
    		boolean isEndPacket = isCallProcedure ? decrementOkCountBy(1): decrementCountBy(1);
    		if (isEndPacket && isCanClose2Client) {
    			
    			if (this.autocommit && !session.getSource().isLocked()) {// clear all connections
    				session.releaseConnections(false);
    			}
    			
    			if (this.isFail() || session.closed()) {
    				tryErrorFinished(true);
    				return;
    			}
    			
    			lock.lock();
    			try {
    				if (rrs.isLoadData()) {
    					byte lastPackId = source.getLoadDataInfileHandler()
    							.getLastPackId();
    					ok.packetId = ++lastPackId;// OK_PACKET
    					ok.message = ("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0")
    							.getBytes();// 此处信息只是为了控制台给人看的
    					source.getLoadDataInfileHandler().clear();
    				} else {
    					ok.packetId = ++packetId;// OK_PACKET
    				}
    
    				ok.affectedRows = affectedRows;
    				ok.serverStatus = source.isAutocommit() ? 2 : 1;
    				if (insertId > 0) {
    					ok.insertId = insertId;
    					source.setLastInsertId(insertId);
    				}
    				
    				ok.write(source);
    			} catch (Exception e) {
    				handleDataProcessException(e);
    			} finally {
    				lock.unlock();
    			}
    		}
    
    
    
    
    
