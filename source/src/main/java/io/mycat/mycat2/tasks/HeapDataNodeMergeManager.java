package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.PackWraper;
import io.mycat.mycat2.beans.ColumnMeta;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.mpp.MergeColumn;
import io.mycat.mycat2.mpp.OrderCol;
import io.mycat.mycat2.mpp.RowDataPacketGrouper;
import io.mycat.mycat2.mpp.RowDataSorter;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import io.mycat.util.PacketUtil;
import io.mycat.util.StringUtil;
import org.apache.log4j.Logger;

import java.util.*;

public class HeapDataNodeMergeManager extends DataNodeManager {
	private static Logger LOGGER = Logger.getLogger(HeapDataNodeMergeManager.class);
	TableMeta tableMeta;

	private Map<String, LinkedList<RowDataPacket>> result = new HashMap<String, LinkedList<RowDataPacket>>();

	private RowDataSorter sorter;
	private RowDataPacketGrouper grouper;

	private volatile boolean fieldsReturned;

	private LinkedList<RowDataPacket> list = new LinkedList<>();
	Set<Map.Entry<String, ColumnMeta>> entries;
	
	Set<String> shouldRemoveAvgField = new HashSet<>();
	Set<String> shouldRenameAvgField = new HashSet<>();
	
	public HeapDataNodeMergeManager(RouteResultset rrs, MycatSession mycatSession) {
		super(rrs, mycatSession);
		for (RouteResultsetNode node : rrs.getNodes()) {
			result.put(node.getName(), new LinkedList<RowDataPacket>());
		}
		fieldsReturned = false;
	}
	
	@Override
	public void onRowMetaData(String datanode, Map<String, ColumnMeta> columToIndx, int fieldCount) {
        System.out.println(columToIndx);
		synchronized (this) {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;
		}
		entries = columToIndx.entrySet();
		
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;  

		if (routeResultset.getGroupByCols() != null) {

			groupColumnIndexs = toColumnIndex(routeResultset.getGroupByCols(), columToIndx);
		}

 		if (routeResultset.getHavingCols() != null) {
			ColumnMeta columnMeta = columToIndx.get(routeResultset.getHavingCols().getLeft().toUpperCase());
			if (columnMeta != null) {
				routeResultset.getHavingCols().setColumnMeta(columnMeta);
			}
		}

		if (routeResultset.isHasAggrColumn()) {
			List<MergeColumn> mergCols = new LinkedList<MergeColumn>();
			Map<String, Integer> mergeColumnsMap = routeResultset.getMergeColumns();

			if (mergeColumnsMap != null) {
				for (Map.Entry<String, Integer> mergEntry : mergeColumnsMap.entrySet()) {
					String colName = mergEntry.getKey().toUpperCase();
					int type = mergEntry.getValue();
					if (MergeColumn.MERGE_AVG == type) {

						ColumnMeta sumColumnMeta = columToIndx.get(colName + "SUM");
						ColumnMeta countColumnMeta = columToIndx.get(colName + "COUNT");
						if (sumColumnMeta != null && countColumnMeta != null) {
							ColumnMeta columnMeta = new ColumnMeta(sumColumnMeta.colIndex, countColumnMeta.colIndex,
									sumColumnMeta.getColType());
							columnMeta.decimals = sumColumnMeta.decimals; // 保存精度
							mergCols.add(new MergeColumn(columnMeta, mergEntry.getValue()));
							shouldRemoveAvgField.add((colName + "COUNT")
									.toUpperCase());
							shouldRenameAvgField.add((colName + "SUM")
									.toUpperCase());
						}
					} else {

						ColumnMeta columnMeta = columToIndx.get(colName);
						mergCols.add(new MergeColumn(columnMeta, mergEntry.getValue()));
					}
				}
			}
			// add no alias merg column
			for (Map.Entry<String, ColumnMeta> fieldEntry : columToIndx.entrySet()) {
				String colName = fieldEntry.getKey();
				int result = MergeColumn.tryParseAggCol(colName);
				if (result != MergeColumn.MERGE_UNSUPPORT && result != MergeColumn.MERGE_NOMERGE) {
					mergCols.add(new MergeColumn(fieldEntry.getValue(), result));
				}
			}

			grouper = new RowDataPacketGrouper(groupColumnIndexs, mergCols.toArray(new MergeColumn[mergCols.size()]),
					routeResultset.getHavingCols());
		}

		if (routeResultset.getOrderByCols() != null) {
			LinkedHashMap<String, Integer> orders = routeResultset.getOrderByCols();
			OrderCol[] orderCols = new OrderCol[orders.size()];
			int i = 0;
			for (Map.Entry<String, Integer> entry : orders.entrySet()) {
				String key = StringUtil.removeBackquote(entry.getKey());
				ColumnMeta colMeta = columToIndx.get(key);
				if (colMeta == null) {
					throw new IllegalArgumentException(
							"all columns in order by clause should be in the selected column list!"
									+ entry.getKey());
				}
				orderCols[i++] = new OrderCol(colMeta, entry.getValue());
			}

			RowDataSorter tmp = new RowDataSorter(orderCols);
			tmp.setLimit(routeResultset.getLimitStart(), routeResultset.getLimitSize());
			sorter = tmp;
		}
	}

	public TableMeta getTableMeta() {
		return tableMeta;
	}

	public void clear() {
		this.tableMeta = null;
        //this.mycatSession.merge = null;
		result.clear();
		grouper = null;
		sorter = null;
	}

	@Override
	public void run() {
		if (!running.compareAndSet(false, true)) {
			return;
		}
		// eof handler has been placed to "if (pack == END_FLAG_PACK){}" in
		// for-statement
		// @author Uncle-pan
		// @since 2016-03-23
		boolean nulpack = false;
		try {
			// loop-on-packs
			for (;;) {
                //@todo it may be a bug
                if (packs == null) {
                    return;
                }
				final PackWraper pack = packs.take();
				if (pack == null) {
					nulpack = true;
					break;
				}

				if (pack == END_FLAG_PACK) {
					fieldCount  = fieldCount-shouldRemoveAvgField.size();
					ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
					FieldPacket[] fields = new FieldPacket[fieldCount];
					EOFPacket eof = new EOFPacket();

					int i = 0;
					byte packetId = 0;
					header.packetId = ++packetId;
										
					for (Map.Entry<String, ColumnMeta> entry : entries) {
						String fieldName = entry.getKey();
						if (shouldRenameAvgField.contains(entry.getKey())) {
							fieldName = fieldName.substring(0,
									fieldName.length() - 3);
						}
						fields[i] = PacketUtil.getField(fieldName, entry.getValue().colType);
						fields[i++].packetId = ++packetId;
					}
					eof.packetId = ++packetId;

					ProxyBuffer buffer = mycatSession.proxyBuffer;
					buffer.reset();

					// write header
					header.write(buffer);

					// write fields
					for (FieldPacket field : fields) {
                        //@todo it must be a bug
                        if (field == null) {
                            continue;
                        }

						field.write(buffer);
					}
					// write eof
					eof.write(buffer);

					// write rows
					packetId = eof.packetId;

					// 处理limit语句
					int start = routeResultset.getLimitStart();
					int end = start + routeResultset.getLimitSize();

					if (start < 0) {
						start = 0;
					}
					List<RowDataPacket> results = getResults();
					if (routeResultset.getLimitSize() < 0) {
						end =results.size();
					}
					
					for (int j = start; j < end; j++) {
						RowDataPacket row = results.get(j);
						row.packetId = ++packetId;
						row.write(buffer);
					}

					// write last eof
					EOFPacket lastEof = new EOFPacket();
					lastEof.packetId = ++packetId;
					lastEof.write(buffer);

					buffer.flip();
					buffer.readIndex = buffer.writeIndex;
					mycatSession.writeToChannel();
					break;
				} else {
					RowDataPacket row = new RowDataPacket(fieldCount);
					ProxyBuffer proxyBuffer = new ProxyBuffer(pack.rowData);
					for (int i1 = 0; i1 < fieldCount; i1++) {
						byte[] value = proxyBuffer.readLenencBytes();
						row.add(value);
					}
					list.add(row);
					if (grouper != null) {
						grouper.addRow(row);
					} else if (sorter != null) {
						sorter.addRow(row);
					}else {
						result.get(pack.dataNode).add(row);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			this.closeMutilBackendAndResponseError(false, ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
		} finally {
			running.set(false);
		}
		// try to check packs, it's possible that adding a pack after polling a
		// null pack
		// and before this time pointer!!
		// @author Uncle-pan
		// @since 2016-03-23
		if (nulpack && !packs.isEmpty()) {
			this.run();
		}
	}

	@Override
	public void onError(String dataNode, String msg) {
		this.closeMutilBackendAndResponseError(false, ErrorCode.ER_UNKNOWN_ERROR, msg);
	}

	@Override
	public void onfinished() {
		clearSQLQueryStreamResouces();
	}

	public List<RowDataPacket> getResults() {

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

		// no grouper and sorter
		if (tmpResult == null) {
			tmpResult = new LinkedList<RowDataPacket>();
			for (RouteResultsetNode node : routeResultset.getNodes()) {
				tmpResult.addAll(result.get(node.getName()));
			}
			// tmpResult = list;
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("prepare mpp merge result for " + routeResultset.getStatement());
		}
		return tmpResult;
	}
}
