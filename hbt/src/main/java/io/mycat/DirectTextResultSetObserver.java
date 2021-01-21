///**
// * Copyright (C) <2019>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat;
//
//import io.mycat.api.collector.RowBaseIterator;
//import io.mycat.beans.mycat.MycatRowMetaData;
//import io.reactivex.rxjava3.annotations.NonNull;
//
//import java.sql.Types;
//import java.util.Iterator;
//import java.util.Objects;
//
///**
// * @author Junwen Chen
// **/
//public class DirectTextResultSetObserver extends MycatResultSetObserver {
//
//    public DirectTextResultSetObserver(MycatRowMetaData rowMetaData,  response) {
//        super(rowMetaData, response);
//    }
//
//    @Override
//    public void onNext(Object @NonNull [] objects) {
//        byte[][] row = new byte[columnCount][];
//        for (int columnIndex = 0, rowIndex = 0; rowIndex < columnCount; columnIndex++, rowIndex++) {
//
//            int columnType = rowMetaData.getColumnType(columnIndex);
//            switch (columnType) {
//                case Types.VARBINARY:
//                case Types.LONGVARBINARY:
//                case Types.BINARY:
//                    byte[] bytes = (byte[]) objects[columnIndex];
//                    row[rowIndex] = bytes ==null? null : bytes;
//                    break;
//                default:
//                    Object object = objects[columnIndex];
//                    row[rowIndex] =(object==null?null: Objects.toString(object).getBytes());
//                    break;
//            }
//
//        }
//    }
//}