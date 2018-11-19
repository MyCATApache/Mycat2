package io.mycat.mysql;

/**
 * cjw
 * 294712221@qq.com
 */
public class CapabilityFlags {
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
        return isLongPassword(value);
    }

    public static boolean isLongPassword(int value) {
        return (value & Capabilities.CLIENT_LONG_PASSWORD) != 0;
    }

    public void setLongPassword() {
        value |= Capabilities.CLIENT_LONG_PASSWORD;
    }

    public boolean isFoundRows() {
        return isFoundRows(value);
    }

    public static boolean isFoundRows(int value) {
        return (value & Capabilities.CLIENT_FOUND_ROWS) != 0;
    }

    public void setFoundRows() {
        value |= Capabilities.CLIENT_FOUND_ROWS;
    }

    public boolean isLongColumnWithFLags() {
        return isLongColumnWithFLags(value);
    }

    public boolean isLongColumnWithFLags(int value) {
        return (value & Capabilities.CLIENT_LONG_FLAG) != 0;
    }

    public void setLongColumnWithFLags() {
        value |= Capabilities.CLIENT_LONG_FLAG;
    }

    public boolean isConnectionWithDatabase() {
        return isConnectionWithDatabase(value);
    }

    public static boolean isConnectionWithDatabase(int value) {
        return (value & Capabilities.CLIENT_CONNECT_WITH_DB) != 0;
    }

    public void setConnectionWithDatabase() {
        value |= Capabilities.CLIENT_CONNECT_WITH_DB;
    }

    public boolean isDoNotAllowDatabaseDotTableDotColumn() {
        return isDoNotAllowDatabaseDotTableDotColumn(value);
    }

    public static boolean isDoNotAllowDatabaseDotTableDotColumn(int value) {
        return (value & Capabilities.CLIENT_NO_SCHEMA) != 0;
    }

    public void setDoNotAllowDatabaseDotTableDotColumn() {
        value |= Capabilities.CLIENT_NO_SCHEMA;
    }

    public boolean isCanUseCompressionProtocol() {
        return isCanUseCompressionProtocol(value);
    }

    public static boolean isCanUseCompressionProtocol(int value) {
        return (value & Capabilities.CLIENT_COMPRESS) != 0;
    }

    public void setCanUseCompressionProtocol() {
        value |= Capabilities.CLIENT_COMPRESS;
    }

    public boolean isODBCClient() {
        return isODBCClient(value);
    }

    public static boolean isODBCClient(int value) {
        return (value & Capabilities.CLIENT_ODBC) != 0;
    }

    public void setODBCClient() {
        value |= Capabilities.CLIENT_ODBC;
    }

    public boolean isCanUseLoadDataLocal() {
        return isCanUseLoadDataLocal(value);
    }

    public static boolean isCanUseLoadDataLocal(int value) {
        return (value & Capabilities.CLIENT_LOCAL_FILES) != 0;
    }

    public void setCanUseLoadDataLocal() {
        value |= Capabilities.CLIENT_LOCAL_FILES;
    }

    public boolean isIgnoreSpacesBeforeLeftBracket() {
        return isIgnoreSpacesBeforeLeftBracket(value);
    }

    public static boolean isIgnoreSpacesBeforeLeftBracket(int value) {
        return (value & Capabilities.CLIENT_IGNORE_SPACE) != 0;
    }

    public void setIgnoreSpacesBeforeLeftBracket() {
        value |= Capabilities.CLIENT_IGNORE_SPACE;
    }

    public boolean isClientProtocol41() {
        return isClientProtocol41(value);
    }

    public static boolean isClientProtocol41(int value) {
        return (value & Capabilities.CLIENT_PROTOCOL_41) != 0;
    }

    public void setClientProtocol41() {
        value |= Capabilities.CLIENT_PROTOCOL_41;
    }

    public boolean isSwitchToSSLAfterHandshake() {
        return isSwitchToSSLAfterHandshake(value);
    }

    public static boolean isSwitchToSSLAfterHandshake(int value) {
        return (value & Capabilities.CLIENT_SSL) != 0;
    }

    public void setSwitchToSSLAfterHandshake() {
        value |= Capabilities.CLIENT_SSL;
    }

    public boolean isIgnoreSigpipes() {
        return isIgnoreSigpipes(value);
    }

    public static boolean isIgnoreSigpipes(int value) {
        return (value & Capabilities.CLIENT_IGNORE_SIGPIPE) != 0;
    }

    public void setIgnoreSigpipes() {
        value |= Capabilities.CLIENT_IGNORE_SIGPIPE;
    }

    public boolean isKnowsAboutTransactions() {
        return isKnowsAboutTransactions(value);
    }

    public static boolean isKnowsAboutTransactions(int value) {
        return (value & Capabilities.CLIENT_TRANSACTIONS) != 0;
    }

    public void setKnowsAboutTransactions() {
        value |= Capabilities.CLIENT_TRANSACTIONS;
    }


    public void setInteractive() {
        value |= Capabilities.CLIENT_INTERACTIVE;
    }

    public boolean isInteractive() {
        return isInteractive(value);
    }

    public static boolean isInteractive(int value) {
        return (value & Capabilities.CLIENT_INTERACTIVE) != 0;
    }

    public boolean isSpeak41Protocol() {
        return isSpeak41Protocol(value);
    }

    public static boolean isSpeak41Protocol(int value) {
        return (value & Capabilities.CLIENT_RESERVED) != 0;
    }

    public void setSpeak41Protocol() {
        value |= Capabilities.CLIENT_RESERVED;
    }

    public boolean isCanDo41Anthentication() {
        return isCanDo41Anthentication(value);
    }

    public static boolean isCanDo41Anthentication(int value) {
        return (value & Capabilities.CLIENT_SECURE_CONNECTION) != 0;
    }

    public void setCanDo41Anthentication() {
        value |= Capabilities.CLIENT_SECURE_CONNECTION;
    }


    public boolean isMultipleStatements() {
        return isMultipleStatements(value);
    }

    public static boolean isMultipleStatements(int value) {
        return (value & Capabilities.CLIENT_MULTI_STATEMENTS) != 0;
    }

    public void setMultipleStatements() {
        value |= Capabilities.CLIENT_MULTI_STATEMENTS;
    }

    public boolean isMultipleResults() {
        return isMultipleResults(value);
    }

    public boolean isMultipleResults(int value) {
        return (value & Capabilities.CLIENT_MULTI_RESULTS) != 0;
    }

    public void setMultipleResults() {
        value |= Capabilities.CLIENT_MULTI_RESULTS;
    }

    public boolean isPSMultipleResults() {
        return isPSMultipleResults(value);
    }

    public static boolean isPSMultipleResults(int value) {
        return (value & Capabilities.CLIENT_PS_MULTI_RESULTS) != 0;
    }

    public void setPSMultipleResults() {
        value |= Capabilities.CLIENT_PS_MULTI_RESULTS;
    }

    public boolean isPluginAuth() {
        return isPluginAuth(value);
    }

    public static boolean isPluginAuth(int value) {
        return (value & Capabilities.CLIENT_PLUGIN_AUTH) != 0;
    }

    public void setPluginAuth() {
        value |= Capabilities.CLIENT_PLUGIN_AUTH;
    }

    public boolean isConnectAttrs() {
        return isConnectAttrs(value);
    }

    public static boolean isConnectAttrs(int value) {
        return (value & Capabilities.CLIENT_CONNECT_ATTRS) != 0;
    }

    public void setConnectAttrs() {
        value |= Capabilities.CLIENT_CONNECT_ATTRS;
    }

    public boolean isPluginAuthLenencClientData() {
        return isPluginAuthLenencClientData(value);
    }

    public static boolean isPluginAuthLenencClientData(int value) {
        return (value & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0;
    }

    public void setPluginAuthLenencClientData() {
        value |= Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }

    public boolean isClientCanHandleExpiredPasswords() {
        return isClientCanHandleExpiredPasswords(value);
    }

    public boolean isClientCanHandleExpiredPasswords(int value) {
        return (value & Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS) != 0;
    }

    public void setClientCanHandleExpiredPasswords() {
        value |= Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS;
    }

    public boolean isSessionVariableTracking() {
        return isSessionVariableTracking(value);
    }

    public static boolean isSessionVariableTracking(int value) {
        return (value & Capabilities.CLIENT_SESSION_TRACK) != 0;
    }

    public void setSessionVariableTracking() {
        value |= Capabilities.CLIENT_SESSION_TRACK;
    }

    public boolean isDeprecateEOF() {
        return isDeprecateEOF(value);
    }

    public static boolean isDeprecateEOF(int value) {
        return (value & Capabilities.CLIENT_DEPRECATE_EOF) != 0;
    }

    public void setDeprecateEOF() {
        value |= Capabilities.CLIENT_DEPRECATE_EOF;
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


}
