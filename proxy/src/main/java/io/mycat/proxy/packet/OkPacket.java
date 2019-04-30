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
package io.mycat.proxy.packet;

public interface OkPacket{
//    public long affectedRows;
//    public long lastInsertId;
//    public int serverStatus;
//    public int warningCount;
//    public byte[] statusInfo;

    //    public byte sessionStateInfoType;
//    public byte[] sessionStateInfoTypeData;
//    public byte[] message;
    public int getOkAffectedRows();

    public void setOkAffectedRows(int affectedRows);

    public int getOkLastInsertId();

    public void setOkLastInsertId(int lastInsertId);

    public int getOkServerStatus();

    public int setOkServerStatus(int serverStatus);

    public int getOkWarningCount();

    public void setOkWarningCount(int warningCount);

    public byte[] getOkStatusInfo();

    public void setOkStatusInfo(byte[] statusInfo);

    public byte getOkSessionStateInfoType();

    public void setOkSessionStateInfoType(byte sessionStateInfoType);

    public byte[] getOkSessionStateInfoTypeData();

    public void setOkSessionStateInfoTypeData(byte[] sessionStateInfoTypeData);

    public byte[] getOkMessage();

    public void setOkMessage(byte[] message);

}
