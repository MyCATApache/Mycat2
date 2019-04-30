/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session.resultSetProcesss;

public abstract class ResultSet<ColumnDef, RowData,Session> {
    ColumnDef[] columnDefList;
    int columnIndex = 0;
    RowIterator<RowData> rowDataIterator;
    State state = State.COLUMN_COUNT;

    public ResultSet(ColumnDef[] columnDefList, RowIterator<RowData> rowDataIterator) {
        this.columnDefList = columnDefList;
        this.rowDataIterator = rowDataIterator;
    }

    enum State {
        COLUMN_COUNT,
        COLUMN_DEF,
        COULMN_EOF,
        ROW
    }

    public boolean hasFinished() {
        return !rowDataIterator.hasNext();
    }

    public void run(Session mycat) {
        switch (state) {
            case COLUMN_COUNT:
                if (columnDefList.length >0) {
                    writeColunmCountPacket(columnDefList.length, mycat);
                    state = State.COLUMN_DEF;
                }else {
                    state = State.ROW;
                }
                break;
            case COLUMN_DEF:
                writeColunmPacket(columnDefList[columnIndex], mycat);
                columnIndex++;
                if (columnIndex == columnDefList.length){
                    state = State.COULMN_EOF;
                }
                break;
            case COULMN_EOF:
                writeColunmDefEOfPacket(columnDefList, mycat);
                state = State.ROW;
                break;
            case ROW:
                if (rowDataIterator.hasNext()) {
                    RowData rowData = rowDataIterator.next();
                    writeResultRow(rowData, mycat);
                }else {
                }
                break;
        }
    }

   public abstract   void writeColunmDefEOfPacket(ColumnDef[] columnDefList, Session mycat) ;

    public abstract void writeColunmCountPacket(int count, Session mycat) ;

    public  abstract  void writeColunmPacket(ColumnDef columnDefList, Session mycat) ;

    public  abstract void writeResultRow(RowData rowData, Session mycat) ;

    public void close() {
        this.columnDefList = null;
        if (this.rowDataIterator != null){
            this.rowDataIterator.close();
        }
        this.rowDataIterator = null;
    }
}
