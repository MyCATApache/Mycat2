package io.mycat.mysql.packet;

import io.mycat.mysql.Capabilities;
import io.mycat.proxy.ProxyBuffer;

import static io.mycat.mysql.Capabilities.CLIENT_CONNECT_ATTRS;
import static io.mycat.mysql.Capabilities.CLIENT_PLUGIN_AUTH;

public class NewHandshakePacket {
    public byte packetId;
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin data part 1
    public CapabilityFlags capabilities;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public int authPluginDataLen;
    // public String reserved="";
    public String authPluginDataPartTwo;
    public String authPluginName;


    public void readPayload(ProxyBuffer buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new RuntimeException("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skip(1);
        capabilities = new CapabilityFlags((int) buffer.readFixInt(2) & 0x0000ffff);
        if (!buffer.readFinished()) {
            hasPartTwo = true;
            characterSet = Byte.toUnsignedInt(buffer.readByte());
            statusFlags = (int) buffer.readFixInt(2);
            long l = buffer.readFixInt(2) << 16;
            capabilities.value |= l;
            if (capabilities.isPluginAuth()) {
                byte b = buffer.readByte();
                authPluginDataLen = Byte.toUnsignedInt(b);
            } else {
                buffer.skip(1);
            }
            //reserved = buffer.readFixString(10);
            buffer.skip(10);
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
                authPluginDataPartTwo = buffer.readFixString(13);
            }
            if (capabilities.isPluginAuth()) {
                authPluginName = buffer.readNULString();
            }
        }
    }

    public void writePayload(ProxyBuffer buffer) {
        buffer.writeByte((byte) 0x0a);
        buffer.writeNULString(serverVersion);
        buffer.writeFixInt(4, connectionId);
        if (authPluginDataPartOne.length() != 8) {
            throw new RuntimeException("authPluginDataPartOne's length must be 8!");
        }
        buffer.writeFixString(authPluginDataPartOne);
        buffer.writeByte((byte) 0);
        buffer.writeFixInt(2, this.capabilities.getLower2Bytes());
        if (hasPartTwo) {
            buffer.writeByte((byte) characterSet);
            buffer.writeFixInt(2, this.statusFlags);
            buffer.writeFixInt(2, this.capabilities.getUpper2Bytes());
            if (this.capabilities.isPluginAuth()) {
                buffer.writeByte((byte) authPluginDataLen);
            } else {
                buffer.writeByte((byte) 0);
            }
            buffer.writeReserved(10);
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
                buffer.writeFixString(authPluginDataPartTwo);
            }
            if (capabilities.isPluginAuth()) {
                buffer.writeNULString(this.authPluginName);
            }
        }
    }

    public int calcPacketSize() {
//        buffer.writeByte((byte) 0x0a);
//        buffer.writeNULString(serverVersion);
//        buffer.writeFixInt(4, connectionId);
        int size = 0;
        size += 1 + serverVersion.length() + 1 + 4;
//        buffer.writeFixString(authPluginDataPartOne);
//        buffer.writeByte((byte) 0);
//        buffer.writeFixInt(2, this.capabilities.getLower2Bytes());
        size += 8 + 1 + 2;
        if (hasPartTwo) {
//            buffer.writeByte((byte) characterSet);
//            buffer.writeFixInt(2, this.statusFlags);
//            buffer.writeFixInt(2, this.capabilities.getUpper2Bytes());
            size += 1 + 2 + 2;
//            if (this.capabilities.isPluginAuth()) {
//                buffer.writeByte((byte) authPluginDataLen);
//            } else {
//                buffer.writeByte((byte) 0);
//            }
            size += 1;
//            buffer.writeReserved(10);
            size += 10;
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
//                buffer.writeFixString(authPluginDataPartTwo);
                size += authPluginDataPartTwo.length();
            }
            if (capabilities.isPluginAuth()) {
//                buffer.writeNULString(this.authPluginName);
                size += this.authPluginName.length() + 1;
            }
        }
        return size;
    }

    public void read(ProxyBuffer buffer) {
        int packetLength = (int) buffer.readFixInt(3);
        int packetId = buffer.readByte();
        readPayload(buffer);
    }
    public void write(ProxyBuffer buffer) {
        int pkgSize = calcPacketSize();
        buffer.writeFixInt(3, pkgSize);
        buffer.writeByte(packetId);
        writePayload(buffer);
    }


    /**
     * cjw
     * 294712221@qq.com
     */
    public static class CapabilityFlags {
        public int value = 0;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CapabilityFlags{");
            sb.append("value=").append(Integer.toBinaryString(value << 7));
            sb.append('}');
            return sb.toString();
        }

        public CapabilityFlags(int capabilities) {
            this.value = capabilities;
        }

        public CapabilityFlags() {
        }

        public int getLower2Bytes() {
            return value & 0x0000ffff;
        }

        public int getUpper2Bytes() {
            return value >>> 16;
        }

        public boolean isLongPassword() {
            return (value & Capabilities.CLIENT_LONG_PASSWORD) != 0;
        }

        public void setLongPassword() {
            value |= Capabilities.CLIENT_LONG_PASSWORD;
        }

        public boolean isFoundRows() {
            return (value & Capabilities.CLIENT_FOUND_ROWS) != 0;
        }

        public void setFoundRows() {
            value |= Capabilities.CLIENT_FOUND_ROWS;
        }

        public boolean isLongColumnWithFLags() {
            return (value & Capabilities.CLIENT_LONG_FLAG) != 0;
        }

        public void setLongColumnWithFLags() {
            value |= Capabilities.CLIENT_LONG_FLAG;
        }

        public boolean isConnectionWithDatabase() {
            return (value & Capabilities.CLIENT_CONNECT_WITH_DB) != 0;
        }

        public void setConnectionWithDatabase() {
            value |= Capabilities.CLIENT_CONNECT_WITH_DB;
        }

        public boolean isDoNotAllowDatabaseDotTableDotColumn() {
            return (value & Capabilities.CLIENT_NO_SCHEMA) != 0;
        }

        public void setDoNotAllowDatabaseDotTableDotColumn() {
            value |= Capabilities.CLIENT_NO_SCHEMA;
        }

        public boolean isCanUseCompressionProtocol() {
            return (value & Capabilities.CLIENT_COMPRESS) != 0;
        }

        public void setCanUseCompressionProtocol() {
            value |= Capabilities.CLIENT_COMPRESS;
        }

        public boolean isODBCClient() {
            return (value & Capabilities.CLIENT_ODBC) != 0;
        }

        public void setODBCClient() {
            value |= Capabilities.CLIENT_ODBC;
        }

        public boolean isCanUseLoadDataLocal() {
            return (value & Capabilities.CLIENT_LOCAL_FILES) != 0;
        }

        public void setCanUseLoadDataLocal() {
            value |= Capabilities.CLIENT_LOCAL_FILES;
        }

        public boolean isIgnoreSpacesBeforeLeftBracket() {
            return (value & Capabilities.CLIENT_IGNORE_SPACE) != 0;
        }

        public void setIgnoreSpacesBeforeLeftBracket() {
            value |= Capabilities.CLIENT_IGNORE_SPACE;
        }

        public boolean isClientProtocol41() {
            return (value & Capabilities.CLIENT_PROTOCOL_41) != 0;
        }


        public void setClientProtocol41() {
            value |= Capabilities.CLIENT_PROTOCOL_41;
        }

        public boolean isSwitchToSSLAfterHandshake() {
            return (value & Capabilities.CLIENT_SSL) != 0;
        }

        public void setSwitchToSSLAfterHandshake() {
            value |= Capabilities.CLIENT_SSL;
        }

        public boolean isIgnoreSigpipes() {
            return (value & Capabilities.CLIENT_IGNORE_SIGPIPE) != 0;
        }

        public void setIgnoreSigpipes() {
            value |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        }

        public boolean isKnowsAboutTransactions() {
            return (value & Capabilities.CLIENT_TRANSACTIONS) != 0;
        }

        public void setKnowsAboutTransactions() {
            value |= Capabilities.CLIENT_TRANSACTIONS;
        }


        public void setInteractive() {
            value |= Capabilities.CLIENT_INTERACTIVE;
        }

        public boolean isInteractive() {
            return (value & Capabilities.CLIENT_INTERACTIVE) != 0;
        }

        public boolean isSpeak41Protocol() {
            return (value & Capabilities.CLIENT_RESERVED) != 0;
        }

        public void setSpeak41Protocol() {
            value |= Capabilities.CLIENT_RESERVED;
        }


        public boolean isCanDo41Anthentication() {
            return (value & Capabilities.CLIENT_SECURE_CONNECTION) != 0;
        }

        public void setCanDo41Anthentication() {
            value |= Capabilities.CLIENT_SECURE_CONNECTION;
        }


        public boolean isMultipleStatements() {
            return (value & Capabilities.CLIENT_MULTI_STATEMENTS) != 0;
        }

        public void setMultipleStatements() {
            value |= Capabilities.CLIENT_MULTI_STATEMENTS;
        }

        public boolean isMultipleResults() {
            return (value & Capabilities.CLIENT_MULTI_RESULTS) != 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CapabilityFlags)) return false;

            CapabilityFlags that = (CapabilityFlags) o;

            return hashCode() == that.hashCode();
        }

        @Override
        public int hashCode() {
            return value << 7;
        }

        public void setMultipleResults() {
            value |= Capabilities.CLIENT_MULTI_RESULTS;
        }

        public boolean isPSMultipleResults() {
            return (value & Capabilities.CLIENT_PS_MULTI_RESULTS) != 0;
        }

        public void setPSMultipleResults() {
            value |= Capabilities.CLIENT_PS_MULTI_RESULTS;
        }

        public boolean isPluginAuth() {
            return (value & CLIENT_PLUGIN_AUTH) != 0;
        }

        public void setPluginAuth() {
            value |= CLIENT_PLUGIN_AUTH;
        }

        public boolean isConnectAttrs() {
            return (value & CLIENT_CONNECT_ATTRS) != 0;
        }

        public void setConnectAttrs() {
            value |= CLIENT_CONNECT_ATTRS;
        }

        public boolean isPluginAuthLenencClientData() {
            return (value & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0;
        }

        public void setPluginAuthLenencClientData() {
            value |= Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        }

        public boolean isClientCanHandleExpiredPasswords() {
            return (value & Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS) != 0;
        }

        public void setClientCanHandleExpiredPasswords() {
            value |= Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS;
        }

        public boolean isSessionVariableTracking() {
            return (value & Capabilities.CLIENT_SESSION_TRACK) != 0;
        }

        public void setSessionVariableTracking() {
            value |= Capabilities.CLIENT_SESSION_TRACK;
        }

        public boolean isDeprecateEOF() {
            return (value & Capabilities.CLIENT_DEPRECATE_EOF) != 0;
        }

        public void setDeprecateEOF() {
            value |= Capabilities.CLIENT_DEPRECATE_EOF;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewHandshakePacket)) return false;

        NewHandshakePacket that = (NewHandshakePacket) o;

        if (protocolVersion != that.protocolVersion) return false;
        if (connectionId != that.connectionId) return false;
        if (hasPartTwo != that.hasPartTwo) return false;
        if (characterSet != that.characterSet) return false;
        if (statusFlags != that.statusFlags) return false;
        if (authPluginDataLen != that.authPluginDataLen) return false;
        if (serverVersion != null ? !serverVersion.equals(that.serverVersion) : that.serverVersion != null)
            return false;
        if (authPluginDataPartOne != null ? !authPluginDataPartOne.equals(that.authPluginDataPartOne) : that.authPluginDataPartOne != null)
            return false;
        if (capabilities != null ? !capabilities.equals(that.capabilities) : that.capabilities != null) return false;
        if (authPluginDataPartTwo != null ? !authPluginDataPartTwo.equals(that.authPluginDataPartTwo) : that.authPluginDataPartTwo != null)
            return false;
        return authPluginName != null ? authPluginName.equals(that.authPluginName) : that.authPluginName == null;
    }

    @Override
    public int hashCode() {
        int result = protocolVersion;
        result = 31 * result + (serverVersion != null ? serverVersion.hashCode() : 0);
        result = 31 * result + (int) (connectionId ^ (connectionId >>> 32));
        result = 31 * result + (authPluginDataPartOne != null ? authPluginDataPartOne.hashCode() : 0);
        result = 31 * result + (capabilities != null ? capabilities.hashCode() : 0);
        result = 31 * result + (hasPartTwo ? 1 : 0);
        result = 31 * result + characterSet;
        result = 31 * result + statusFlags;
        result = 31 * result + authPluginDataLen;
        result = 31 * result + (authPluginDataPartTwo != null ? authPluginDataPartTwo.hashCode() : 0);
        result = 31 * result + (authPluginName != null ? authPluginName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NewHandshakePacket{");
        sb.append("protocolVersion=").append(protocolVersion);
        sb.append(", serverVersion='").append(serverVersion).append('\'');
        sb.append(", connectionId=").append(connectionId);
        sb.append(", authPluginDataPartOne='").append(authPluginDataPartOne).append('\'');
        sb.append(", capabilities=").append(capabilities);
        sb.append(", hasPartTwo=").append(hasPartTwo);
        sb.append(", characterSet=").append(characterSet);
        sb.append(", statusFlags=").append(statusFlags);
        sb.append(", authPluginDataLen=").append(authPluginDataLen);
        sb.append(", authPluginDataPartTwo='").append(authPluginDataPartTwo).append('\'');
        sb.append(", authPluginName='").append(authPluginName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
