package io.mycat.mysql.packet;

import io.mycat.mysql.Capabilities;
import io.mycat.proxy.ProxyBuffer;

import static io.mycat.mysql.Capabilities.CLIENT_PLUGIN_AUTH;

public class NewHandshakePacket {
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin data part 1
    public CapabilityFlags capabilities;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public int authPluginDataLen;
    public String reserved;
    public String authPluginDataPartTwo;
    public String authPluginName;

    public void read(ProxyBuffer buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new RuntimeException("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skip(1);
        capabilities = new CapabilityFlags((int) buffer.readFixInt(2));
        if (!buffer.readFinished()) {
            hasPartTwo = true;
            characterSet = Byte.toUnsignedInt(buffer.readByte());
            statusFlags = (int) buffer.readFixInt(2);
            long l = buffer.readFixInt(2) << 16;
            capabilities.value |=l;
            if (capabilities.isPluginAuth()) {
                byte b = buffer.readByte();
                authPluginDataLen = Byte.toUnsignedInt(b);
            } else {
                buffer.skip(1);
            }
            reserved = buffer.readFixString(10);
            if (capabilities.isCanDo41Anthentication()) {
                authPluginDataPartTwo = buffer.readFixString(Math.min(13, authPluginDataLen));
            }
            if (capabilities.isPluginAuth()) {
                authPluginName = buffer.readNULString();
            }
        }
    }

    public void write(ProxyBuffer buffer) {
        buffer.writeByte((byte) 0x0a);
        buffer.writeNULString(serverVersion);
        buffer.writeFixInt(4, connectionId);
        buffer.writeFixString(authPluginDataPartOne);
        buffer.writeByte((byte) 0);
//        if (partTwoCapabilityFlags == null) {
//            throw new RuntimeException("CapabilityFlags must be existed");
//        }
//        buffer.writeFixInt(2, )

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
            sb.append("value=").append(value);
            sb.append('}');
            return sb.toString();
        }

        public CapabilityFlags(int capabilities) {
            this.value = capabilities;
        }

        public CapabilityFlags() {
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

        public boolean isDoNotAllowDatabaseDotTableDotColumn() {
            return (value & Capabilities.CLIENT_CONNECT_WITH_DB) != 0;
        }

        public void setDoNotAllowDatabaseDotTableDotColumn() {
            value |= Capabilities.CLIENT_CONNECT_WITH_DB;
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
            return (value & CLIENT_PLUGIN_AUTH) != 0;
        }

        public void setConnectAttrs() {
            value |= CLIENT_PLUGIN_AUTH;
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

}
