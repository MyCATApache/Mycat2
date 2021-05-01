/**
 * Copyright (C) <2021>  <zhu qiang>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql.packet;


/**
 * AuthSwitchRequest
 * https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
 *
 * <pre>
 *      1              [fe]
 *      string[NUL]    plugin name
 *      string[EOF]    auth plugin data
 * </pre>
 *
 * @author : zwy
 *  date : 2019/05/21 1:40
 *
 *
 *
 **/
public class AuthSwitchRequestPacket {
    private byte status;
    private String authPluginName;
    private String authPluginData;

    public void readPayload(MySQLPayloadReadView buffer) {
        status = buffer.readByte();
        authPluginName = buffer.readNULString();
        authPluginData = buffer.readNULString();
    }


    public void writePayload(MySQLPayloadWriteView buffer) {
        buffer.writeByte(status);
        buffer.writeNULString(authPluginName);
        buffer.writeNULString(authPluginData);
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }

    public void setAuthPluginName(String authPluginName) {
        this.authPluginName = authPluginName;
    }

    public String getAuthPluginData() {
        return authPluginData;
    }

    public void setAuthPluginData(String authPluginData) {
        this.authPluginData = authPluginData;
    }
}
