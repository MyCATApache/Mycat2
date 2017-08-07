package io.mycat.mycat2.cmd;

public class MySQLClientAuthProcessor {
	  
//	    public boolean handleFullPacket(Connection connection, Object attachment, int packetStartPos, int packetLen, byte type) throws IOException {
//	        MySQLFrontConnection mySQLFrontConnection = (MySQLFrontConnection) connection;
//	        try {
//	            AuthPacket auth = new AuthPacket();
//	            auth.read(mySQLFrontConnection.getDataBuffer());
//
//	            // Fake check user
//	            LOGGER.debug("Check user name. " + auth.user);
//	            if (!auth.user.equals("root")) {
//	                LOGGER.debug("User name error. " + auth.user);
//	                mySQLFrontConnection.failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'");
//	                mySQLFrontConnection.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
//	                return true;
//	            }
//
//	            // Fake check password
//	            LOGGER.debug("Check user password. " + new String(auth.password));
//
//	            // check schema
//	            LOGGER.debug("Check database. " + auth.database);
//
//	            return success(mySQLFrontConnection, auth);
//	        } catch (Throwable e) {
//	            LOGGER.warn("Frontend FrontendAuthenticatingState error:", e);
//	            mySQLFrontConnection.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
//	            return true;
//	        }
//	    }
//
//	    private boolean success(MySQLFrontConnection con, AuthPacket auth) throws IOException {
//	        LOGGER.debug("Login success");
//	        // 设置字符集编码
//	        int charsetIndex = (auth.charsetIndex & 0xff);
//	        final String charset = CharsetUtil.getCharset(charsetIndex);
//	        if (charset == null) {
//	            final String errmsg = "Unknown charsetIndex:" + charsetIndex;
//	            LOGGER.warn(errmsg);
//	            con.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, errmsg);
//	            con.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
//	            return true;
//	        }
//	        LOGGER.debug("charset = {}, charsetIndex = {}", charset, charsetIndex);
//	        con.setCharset(charsetIndex, charset);
//
//	        String db = StringUtil.isEmpty(auth.database)
//	                ? SQLEngineCtx.INSTANCE().getDefaultMycatSchema().getName() : auth.database;
//	        if (!con.setFrontSchema(db)) {
//	            final String errmsg = "No Mycat Schema defined: " + auth.database;
//	            LOGGER.debug(errmsg);
//	            con.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "42000".getBytes(), errmsg);
//	            con.setWriteCompleteListener(() -> {
//	                con.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
//	                con.getNetworkStateMachine().setNextState(ClosingState.INSTANCE);
//	            });
//	        } else {
//	            con.getDataBuffer().writeBytes(AUTH_OK);
//	            con.startTransfer(con.getDataBuffer());
//	            con.getProtocolStateMachine().setNextState(FrontendIdleState.INSTANCE);
//	            interruptIterate();
//	        }
//	        return false;
//	    }
}
