package io.mycat.mysql.packet;

import io.mycat.mysql.Capabilities;
import io.mycat.proxy.ProxyBuffer;

import static io.mycat.mysql.Capabilities.CLIENT_PLUGIN_AUTH;

public class NewHandshakePacket {
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin data part 1
    public PartOneCapabilityFlags partOnecapabilityFlags;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public PartTwoCapabilityFlags partTwoCapabilityFlags;
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
        partOnecapabilityFlags = new PartOneCapabilityFlags((int) buffer.readFixInt(2));
        if (!buffer.readFinished()) {
            hasPartTwo = true;
            characterSet = Byte.toUnsignedInt(buffer.readByte());
            statusFlags = (int) buffer.readFixInt(2);
            partTwoCapabilityFlags = new PartTwoCapabilityFlags(buffer.readFixInt(2));
            if (partTwoCapabilityFlags.isPluginAuth()) {
                byte b = buffer.readByte();
                authPluginDataLen = Byte.toUnsignedInt(b);
            } else {
                buffer.skip(1);
            }
            reserved =  buffer.readFixString(10);
            if (partOnecapabilityFlags.isCanDo41Anthentication()) {
                authPluginDataPartTwo = buffer.readFixString(Math.min(13, authPluginDataLen));
            }
            if (partTwoCapabilityFlags.isPluginAuth()) {
                authPluginName = buffer.readNULString();
            }
        }
    }

    public void write(ProxyBuffer buffer) {

    }

    /**
     * cjw
     * 294712221@qq.com
     */
    public static class PartOneCapabilityFlags {
        public int capabilities = 0;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PartOneCapabilityFlags{");
            sb.append("capabilities=").append(capabilities);
            sb.append('}');
            return sb.toString();
        }

        public PartOneCapabilityFlags(int capabilities) {
            this.capabilities = capabilities;
        }

        public PartOneCapabilityFlags() {
        }

        public boolean isLongPassword() {
            return (capabilities & Capabilities.CLIENT_LONG_PASSWORD) != 0;
        }

        public void setLongPassword() {
            capabilities |= Capabilities.CLIENT_LONG_PASSWORD;
        }

        public boolean isFoundRows() {
            return (capabilities & Capabilities.CLIENT_FOUND_ROWS) != 0;
        }

        public void setFoundRows() {
            capabilities |= Capabilities.CLIENT_FOUND_ROWS;
        }

        public boolean isLongColumnWithFLags() {
            return (capabilities & Capabilities.CLIENT_LONG_FLAG) != 0;
        }

        public void setLongColumnWithFLags() {
            capabilities |= Capabilities.CLIENT_LONG_FLAG;
        }

        public boolean isDoNotAllowDatabaseDotTableDotColumn() {
            return (capabilities & Capabilities.CLIENT_CONNECT_WITH_DB) != 0;
        }

        public void setDoNotAllowDatabaseDotTableDotColumn() {
            capabilities |= Capabilities.CLIENT_CONNECT_WITH_DB;
        }

        public boolean isCanUseCompressionProtocol() {
            return (capabilities & Capabilities.CLIENT_COMPRESS) != 0;
        }

        public void setCanUseCompressionProtocol() {
            capabilities |= Capabilities.CLIENT_COMPRESS;
        }

        public boolean isODBCClient() {
            return (capabilities & Capabilities.CLIENT_ODBC) != 0;
        }

        public void setODBCClient() {
            capabilities |= Capabilities.CLIENT_ODBC;
        }

        public boolean isCanUseLoadDataLocal() {
            return (capabilities & Capabilities.CLIENT_LOCAL_FILES) != 0;
        }

        public void setCanUseLoadDataLocal() {
            capabilities |= Capabilities.CLIENT_LOCAL_FILES;
        }

        public boolean isIgnoreSpacesBeforeLeftBracket() {
            return (capabilities & Capabilities.CLIENT_IGNORE_SPACE) != 0;
        }

        public void setIgnoreSpacesBeforeLeftBracket() {
            capabilities |= Capabilities.CLIENT_IGNORE_SPACE;
        }

        public boolean isSwitchToSSLAfterHandshake() {
            return (capabilities & Capabilities.CLIENT_SSL) != 0;
        }

        public void setSwitchToSSLAfterHandshake() {
            capabilities |= Capabilities.CLIENT_SSL;
        }

        public boolean isIgnoreSigpipes() {
            return (capabilities & Capabilities.CLIENT_IGNORE_SIGPIPE) != 0;
        }

        public void setIgnoreSigpipes() {
            capabilities |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        }

        public boolean isKnowsAboutTransactions() {
            return (capabilities & Capabilities.CLIENT_TRANSACTIONS) != 0;
        }

        public void setKnowsAboutTransactions() {
            capabilities |= Capabilities.CLIENT_TRANSACTIONS;
        }

        public boolean isSpeak41Protocol() {
            return (capabilities & Capabilities.CLIENT_RESERVED) != 0;
        }

        public void setSpeak41Protocol() {
            capabilities |= Capabilities.CLIENT_RESERVED;
        }

        public boolean isCanDo41Anthentication() {
            return (capabilities & Capabilities.CLIENT_SECURE_CONNECTION) != 0;
        }

        public void setCanDo41Anthentication() {
            capabilities |= Capabilities.CLIENT_SECURE_CONNECTION;
        }
    }

    /**
     * cjw
     * 294712221@qq.com
     */
    public static class PartTwoCapabilityFlags {
        public long capabilities = 0;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PartTwoCapabilityFlags{");
            sb.append("capabilities=").append(capabilities);
            sb.append('}');
            return sb.toString();
        }

        public PartTwoCapabilityFlags(long capabilities) {
            this.capabilities = capabilities;
        }

        public PartTwoCapabilityFlags() {
        }

        public boolean isMultipleStatements() {
            return (capabilities & Capabilities.CLIENT_MULTI_STATEMENTS) != 0;
        }

        public void setMultipleStatements() {
            capabilities |= Capabilities.CLIENT_MULTI_STATEMENTS;
        }

        public boolean isMultipleResults() {
            return (capabilities & Capabilities.CLIENT_MULTI_RESULTS) != 0;
        }

        public void setMultipleResults() {
            capabilities |= Capabilities.CLIENT_MULTI_RESULTS;
        }

        public boolean isPSMultipleResults() {
            return (capabilities & Capabilities.CLIENT_PS_MULTI_RESULTS) != 0;
        }

        public void setPSMultipleResults() {
            capabilities |= Capabilities.CLIENT_PS_MULTI_RESULTS;
        }

        public boolean isPluginAuth() {
            return (capabilities & CLIENT_PLUGIN_AUTH) != 0;
        }

        public void setPluginAuth() {
            capabilities |= CLIENT_PLUGIN_AUTH;
        }

        public boolean isConnectAttrs() {
            return (capabilities & CLIENT_PLUGIN_AUTH) != 0;
        }

        public void setConnectAttrs() {
            capabilities |= CLIENT_PLUGIN_AUTH;
        }

        public boolean isPluginAuthLenencClientData() {
            return (capabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0;
        }

        public void setPluginAuthLenencClientData() {
            capabilities |= Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        }

        public boolean isClientCanHandleExpiredPasswords() {
            return (capabilities & Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS) != 0;
        }

        public void setClientCanHandleExpiredPasswords() {
            capabilities |= Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS;
        }

        public boolean isSessionVariableTracking() {
            return (capabilities & Capabilities.CLIENT_SESSION_TRACK) != 0;
        }

        public void setSessionVariableTracking() {
            capabilities |= Capabilities.CLIENT_SESSION_TRACK;
        }

        public boolean isDeprecateEOF() {
            return (capabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0;
        }

        public void setDeprecateEOF() {
            capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
        }
    }


}
