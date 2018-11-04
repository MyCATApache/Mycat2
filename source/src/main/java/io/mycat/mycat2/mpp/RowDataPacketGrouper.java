/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2.mpp;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.mycat.mycat2.beans.ColumnMeta;
import io.mycat.mycat2.mpp.util.ByteUtil;
import io.mycat.mycat2.mpp.util.CompareUtil;
import io.mycat.mycat2.mpp.util.LongUtil;
import io.mycat.mysql.packet.RowDataPacket;

/**
 * implement group function select a,count(*),sum(*) from A group by a
 * 
 * @author wuzhih
 * 
 */
public class RowDataPacketGrouper {

	private List<RowDataPacket> result = Collections.synchronizedList(new ArrayList<RowDataPacket>());
	private final MergeColumn[] mergCols;
	private int[] MergeColumnsIndex;
	private final int[] groupColumnIndexs;
	private boolean ishanlderFirstRow = false;   //结果集汇聚时,是否已处理第一条记录.
	private boolean isMergAvg=false;
	private HavingCols havingCols;

	public RowDataPacketGrouper(int[] groupColumnIndexs, MergeColumn[] mergCols,HavingCols havingCols) {
		super();
		this.groupColumnIndexs = groupColumnIndexs;
		this.mergCols = mergCols;
		this.havingCols = havingCols;
		
		if(mergCols!=null&&mergCols.length>0){
			MergeColumnsIndex = new int[mergCols.length];
			for(int i = 0;i<mergCols.length;i++){
				MergeColumnsIndex[i] = mergCols[i].columnMeta.colIndex;
			}
			Arrays.sort(MergeColumnsIndex);
		}
	}

	public List<RowDataPacket> getResult() {
		if(!isMergAvg)
		{
			for (RowDataPacket row : result)
			{
				mergAvg(row);
			}
			isMergAvg=true;
		}

		if(havingCols != null){
			filterHaving();
		}

		return result;
	}

	private void filterHaving(){
		if (havingCols.getColumnMeta() == null || result == null) {
			return;
		}
		Iterator<RowDataPacket> it = result.iterator();
		byte[] right = havingCols.getRight().getBytes(
				StandardCharsets.UTF_8);
		int index = havingCols.getColumnMeta().getColIndex();
		int colType = havingCols.getColumnMeta().getColType();	// Added by winbill. 20160312.
		while (it.hasNext()){
			RowDataPacket rowDataPacket = it.next();
			switch (havingCols.getOperator()) {
			case "=":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (eq(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			case ">":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (gt(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			case "<":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (lt(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			case ">=":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (gt(rowDataPacket.fieldValues.get(index),right,colType) && eq(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			case "<=":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (lt(rowDataPacket.fieldValues.get(index),right,colType) && eq(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			case "!=":
				/* Add parameter of colType, Modified by winbill. 20160312. */
				if (neq(rowDataPacket.fieldValues.get(index),right,colType)) {
					it.remove();
				}
				break;
			}
		}

	}

	/* 
	 * Using new compare function instead of compareNumberByte 
	 * Modified by winbill. 20160312.
	 */
	private boolean lt(byte[] l, byte[] r, final int colType) {
//		return -1 != ByteUtil.compareNumberByte(l, r);
		return -1 != RowDataPacketGrouper.compareObject(l, r, colType);
	}

	private boolean gt(byte[] l, byte[] r, final int colType) {
//		return 1 != ByteUtil.compareNumberByte(l, r, havingCol);
		return 1 != RowDataPacketGrouper.compareObject(l, r, colType);
	}

	private boolean eq(byte[] l, byte[] r, final int colType) {
//		return 0 != ByteUtil.compareNumberByte(l, r, havingCol);
		return 0 != RowDataPacketGrouper.compareObject(l, r, colType);
	}

	private boolean neq(byte[] l, byte[] r, final int colType) {
//		return 0 == ByteUtil.compareNumberByte(l, r, havingCol);
		return 0 == RowDataPacketGrouper.compareObject(l, r, colType);
	}

	/*
	 * Compare with the value of having column
	 * winbill. 20160312.
	 */
    public static final int compareObject(byte[] left,byte[] right, final int colType) {
        switch (colType) {
        case ColumnMeta.COL_TYPE_SHORT:
        case ColumnMeta.COL_TYPE_INT:
        case ColumnMeta.COL_TYPE_INT24:
		case ColumnMeta.COL_TYPE_LONG:
			return CompareUtil.compareInt(ByteUtil.getInt(left), ByteUtil.getInt(right));
        case ColumnMeta.COL_TYPE_LONGLONG:
            return CompareUtil.compareLong(ByteUtil.getLong(left), ByteUtil.getLong(right));
        case ColumnMeta.COL_TYPE_FLOAT:
        case ColumnMeta.COL_TYPE_DOUBLE:
        case ColumnMeta.COL_TYPE_DECIMAL:
        case ColumnMeta.COL_TYPE_NEWDECIMAL:
            return CompareUtil.compareDouble(ByteUtil.getDouble(left), ByteUtil.getDouble(right));
        case ColumnMeta.COL_TYPE_DATE:
        case ColumnMeta.COL_TYPE_TIMSTAMP:
        case ColumnMeta.COL_TYPE_TIME:
        case ColumnMeta.COL_TYPE_YEAR:
        case ColumnMeta.COL_TYPE_DATETIME:
        case ColumnMeta.COL_TYPE_NEWDATE:
        case ColumnMeta.COL_TYPE_BIT:
        case ColumnMeta.COL_TYPE_VAR_STRING:
        case ColumnMeta.COL_TYPE_STRING:
        // ENUM和SET类型都是字符串，按字符串处理
        case ColumnMeta.COL_TYPE_ENUM:
        case ColumnMeta.COL_TYPE_SET:
            return ByteUtil.compareNumberByte(left, right);
        // BLOB相关类型和GEOMETRY类型不支持排序，略掉
        }
        return 0;
    }

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

	private void aggregateRow(RowDataPacket toRow, RowDataPacket newRow) {
		if (mergCols == null) {
			return;
		}
		
		/*
		 * 这里进行一次判断, 在跨分片聚合的情况下,如果有一个没有记录的分片，最先返回,可能返回有null 的情况.
		 */
		if(!ishanlderFirstRow&&MergeColumnsIndex!=null&&MergeColumnsIndex.length>0){
			List<byte[]> values = toRow.fieldValues;
            for(int i=0;i<values.size();i++){
            	if(Arrays.binarySearch(MergeColumnsIndex, i)>=0){
            		continue;
            	}
	           if(values.get(i)==null){
	           	   values.set(i, newRow.fieldValues.get(i));
	           }
            }
            ishanlderFirstRow = true;
		}
		
		for (MergeColumn merg : mergCols) {
             if(merg.mergeType!=MergeColumn.MERGE_AVG)
             {
                 byte[] result = mertFields(
                         toRow.fieldValues.get(merg.columnMeta.colIndex),
                         newRow.fieldValues.get(merg.columnMeta.colIndex),
                         merg.columnMeta.colType, merg.mergeType);
                 if (result != null)
                 {
                     toRow.fieldValues.set(merg.columnMeta.colIndex, result);
                 }
             }
		}
    }

	private void mergAvg(RowDataPacket toRow) {
		if (mergCols == null) {
			return;
		}
		
		

		Set<Integer> rmIndexSet = new HashSet<Integer>();
		for (MergeColumn merg : mergCols) {
			if(merg.mergeType==MergeColumn.MERGE_AVG)
			{
				byte[] result = mertFields(
						toRow.fieldValues.get(merg.columnMeta.avgSumIndex),
						toRow.fieldValues.get(merg.columnMeta.avgCountIndex),
						merg.columnMeta.colType, merg.mergeType);
				if (result != null)
				{
					toRow.fieldValues.set(merg.columnMeta.avgSumIndex, result);
//					toRow.fieldValues.remove(merg.ColumnMeta.avgCountIndex) ;
//					toRow.fieldCount=toRow.fieldCount-1;
					rmIndexSet.add(merg.columnMeta.avgCountIndex);
				}
			}
		}
		for(Integer index : rmIndexSet) {
			toRow.fieldValues.remove(index);
			toRow.fieldCount = toRow.fieldCount - 1;
		}


	}

	private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) {
		// System.out.println("mergeType:"+ mergeType+" colType "+colType+
		// " field:"+Arrays.toString(bs)+ " ->  "+Arrays.toString(bs2));
		if(bs2==null || bs2.length==0)
		{
			return bs;
		}else if(bs==null || bs.length==0)
		{
			return bs2;
		}
		switch (mergeType) {
		case MergeColumn.MERGE_SUM:
			if (colType == ColumnMeta.COL_TYPE_DOUBLE
				|| colType == ColumnMeta.COL_TYPE_FLOAT) {

				Double vale = ByteUtil.getDouble(bs) + ByteUtil.getDouble(bs2);
				return vale.toString().getBytes();
				// return String.valueOf(vale).getBytes();
			} else if(colType == ColumnMeta.COL_TYPE_NEWDECIMAL
					|| colType == ColumnMeta.COL_TYPE_DECIMAL) {
				BigDecimal d1 = new BigDecimal(new String(bs));
				d1 = d1.add(new BigDecimal(new String(bs2)));
				return String.valueOf(d1).getBytes();
			}
			// continue to count case
		case MergeColumn.MERGE_COUNT: {
			long s1 = Long.parseLong(new String(bs));
			long s2 = Long.parseLong(new String(bs2));
			long total = s1 + s2;
			return LongUtil.toBytes(total);
		}
		case MergeColumn.MERGE_MAX: {
			// System.out.println("value:"+
			// ByteUtil.getNumber(bs).doubleValue());
			// System.out.println("value2:"+
			// ByteUtil.getNumber(bs2).doubleValue());
			// int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
			// .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
			// return ByteUtil.compareNumberByte(bs, bs2);
			int compare = ByteUtil.compareNumberByte(bs, bs2);
			return (compare > 0) ? bs : bs2;

		}
		case MergeColumn.MERGE_MIN: {
			// int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
			// .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
			// int compare = ByteUtil.compareNumberArray(bs, bs2);
			//return (compare > 0) ? bs2 : bs;
			int compare = ByteUtil.compareNumberByte(bs, bs2);
			return (compare > 0) ? bs2 : bs;
			// return ByteUtil.compareNumberArray2(bs, bs2, 2);
		}
            case MergeColumn.MERGE_AVG: {
            	if (colType == ColumnMeta.COL_TYPE_DOUBLE
    					|| colType == ColumnMeta.COL_TYPE_FLOAT) {
            		double aDouble = ByteUtil.getDouble(bs);
            		long s2 = Long.parseLong(new String(bs2));
            		Double vale = aDouble / s2;
            		return vale.toString().getBytes();
            	} else if(colType == ColumnMeta.COL_TYPE_NEWDECIMAL
    					|| colType == ColumnMeta.COL_TYPE_DECIMAL) {
            		BigDecimal sum = new BigDecimal(new String(bs));
                    // mysql avg 处理精度为 sum结果的精度扩展4, 采用四舍五入
                    BigDecimal avg = sum.divide(new BigDecimal(new String(bs2)), sum.scale() + 4, RoundingMode.HALF_UP);
                    return avg.toString().getBytes();
            	}
            }
		default:
			return null;
		}

	}

	// private static final

	private boolean sameGropuColums(RowDataPacket newRow, RowDataPacket existRow) {
		if (groupColumnIndexs == null) {// select count(*) from aaa , or group
										// column
			return true;
		}
		for (int i = 0; i < groupColumnIndexs.length; i++) {
			if (!Arrays.equals(newRow.fieldValues.get(groupColumnIndexs[i]),
					existRow.fieldValues.get(groupColumnIndexs[i]))) {
				return false;
			}

		}
		return true;

	}
}
