MergeCol: 聚合方法
ColMeta:聚合方法以及聚合类型
RowDataPacketGrouper:数据汇聚类

mycat结果集合并主要有

我们这里分析堆内即DataMergeService，他是一个实现了Runnable接口的类，这里看到，只有操作了三个方法onRowMetaData，onNewRecord，outputMergeResult，其中只有onRowMetaData是自己实现的，其他两个都是父类AbstractDataNodeMerge的。我们重点看onRowMetaData这个方法。
这个方法有两个参数columToIndx,fieldCount，分别为列的集合，结果集数量，这个方法调用了

	grouper = new RowDataPacketGrouper(groupColumnIndexs,
	mergCols.toArray(new MergeCol[mergCols.size()]),
	rrs.getHavingCols());
	
这个方法里，也有order by语句的处理。

初始化好了数据汇聚类
那接下来我们看onNewRecord,这个方法主要是处理新进来每个row数据，通过PackWraper进行封装，该方法调用了addPack这个方法。在这个方法里面
 
	protected final boolean addPack(final PackWraper pack){
		packs.add(pack);
		if(running.get()){
		    return false;
		}
		final MycatServer server = MycatServer.getInstance();
		server.getBusinessExecutor().execute(this);
		return true;
	    }
调用了自己的run方法，
	
	for (; ; ) {
		//从rowdata缓存队列中取出数据包
		final PackWraper pack = packs.poll();
		// async: handling row pack queue, this business thread should exit when no pack
		// @author Uncle-pan
		// @since 2016-03-23
		if(pack == null){
			nulpack = true;
			break;
		}
		// eof: handling eof pack and exit
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
			multiQueryHandler.outputMergeResult(source, array, getResults(array));
			break;
		}


		// merge: sort-or-group, or simple add
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
	}
如果没结束，结果集合row中，然后调用grouper.addRow(row);这个方法主要是合并相同分组

	public void addRow(RowDataPacket rowDataPkg) {
		for (RowDataPacket row : result) {
			if (sameGropuColums(rowDataPkg, row)) {
				aggregateRow(row, rowDataPkg);
				return;
			}
		}
		// not aggreated ,insert new
		result.add(rowDataPkg);
	}
	
当结束时，就用到了上面的第三个方法outputMergeResult,该方法最终也会到达这里调用if (pack == END_FLAG_PACK) ，在这里调用了一个主要的方法getResults(array)，

	public List<RowDataPacket> getResults(byte[] eof) {
	
		List<RowDataPacket> tmpResult = null;

		if (this.grouper != null) {
			tmpResult = grouper.getResult();
			grouper = null;
		}

		
		if (sorter != null) {
			
			if (tmpResult != null) {
				Iterator<RowDataPacket> itor = tmpResult.iterator();
				while (itor.hasNext()) {
					sorter.addRow(itor.next());
					itor.remove();
				}
			}
			tmpResult = sorter.getSortedResult();
			sorter = null;
		}


		
		//no grouper and sorter
		if(tmpResult == null){
			tmpResult = new LinkedList<RowDataPacket>();
			for (RouteResultsetNode node : rrs.getNodes()) {
				tmpResult.addAll(result.get(node.getName()));
			}
		}
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("prepare mpp merge result for " + rrs.getStatement());
		}
		return tmpResult;
	}
该方法就是执行的RowDataPacketGrouper类中的getResult方法，然后处理grouper处理后的数据交给multiQueryHandler.outputMergeResult();
multiQueryHandler.outputMergeResult(),返回，写入返回给client。

