/**
 * Copyright (C) <2019>  <mycat>
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
package io.mycat.proxy.packet;

public interface ErrorPacket {
     static final byte SQLSTATE_MARKER = (byte) '#';
     static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    public int getErrorStage() ;

    public void setErrorStage(int stage);

    public int getErrorMaxStage() ;

    public void setErrorMaxStage(int maxStage) ;

    public int getErrorProgress() ;

    public void setErrorProgress(int progress) ;

    public byte[] getErrorProgressInfo() ;
    public void setErrorProgressInfo(byte[] progress_info);

    public byte getErrorMark() ;

    public void setErrorMark(byte mark) ;

    public byte[] getErrorSqlState() ;

    public void setErrorSqlState(byte[] sqlState);

    public byte[] getErrorMessage() ;

    public void setErrorMessage(byte[] message) ;

}
