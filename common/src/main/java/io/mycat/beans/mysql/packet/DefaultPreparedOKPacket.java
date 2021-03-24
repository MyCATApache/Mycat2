/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql.packet;

public class DefaultPreparedOKPacket implements PreparedOKPacket {
    long pstmt;
    int columnCount;
    int parametersCount;
    int warningCount;

    public DefaultPreparedOKPacket(long pstmt, int columnCount, int parametersCount, int warningCount) {
        this.pstmt = pstmt;
        this.columnCount = columnCount;
        this.parametersCount = parametersCount;
        this.warningCount = warningCount;
    }

    @Override
    public long getPreparedOkStatementId() {
        return pstmt;
    }

    @Override
    public void setPreparedOkStatementId(long statementId) {
        this.pstmt = statementId;
    }

    @Override
    public int getPrepareOkColumnsCount() {
        return columnCount;
    }

    @Override
    public void setPrepareOkColumnsCount(int columnsNumber) {
        this.columnCount = columnsNumber;
    }

    @Override
    public int getPrepareOkParametersCount() {
        return parametersCount;
    }

    @Override
    public void setPrepareOkParametersCount(int parametersNumber) {
        this.parametersCount = parametersNumber;
    }

    @Override
    public int getPreparedOkWarningCount() {
        return warningCount;
    }

    @Override
    public void setPreparedOkWarningCount(int warningCount) {
        this.warningCount = warningCount;

    }
}