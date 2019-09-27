package io.mycat.command;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.Instruction;
import io.mycat.lib.Response;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.pattern.PatternRuntime;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;

import java.nio.ByteBuffer;

public class PatternCommandHandler extends AbstractCommandHandler{
    DynamicSQLMatcher matcher;

    static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(PatternCommandHandler.class);
    @Override
    public void initRuntime(MycatSession session, ProxyRuntime runtime) {
        matcher  = PatternRuntime.INSTANCE.createMatcher();
    }

    @Override
    public void handleQuery(byte[] sql, MycatSession session) {
        String sqlText = new String(sql);
        LOGGER.debug(sqlText);
        Instruction match = matcher.match(sqlText);
        if (match != null){
            try {
                Response response = match.execute(session, matcher);
                response.apply(session, matcher);
            }catch (Exception e){
                LOGGER.error("{}",e);
                throw e;
            }
        }else {
            LOGGER.error("unknown sql:{}",sqlText);
            session.setLastMessage("unknown sql:"+sqlText);
            session.writeErrorEndPacket();
        }
    }

    @Override
    public void handleContentOfFilename(byte[] sql, MycatSession session) {

    }

    @Override
    public void handleContentOfFilenameEmptyOk(MycatSession session) {

    }

    @Override
    public void handlePrepareStatement(byte[] sql, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int numParams, byte[] rest, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementClose(long statementId, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementReset(long statementId, MycatSession session) {

    }

    @Override
    public int getNumParamsByStatementId(long statementId, MycatSession session) {
        return 0;
    }
}