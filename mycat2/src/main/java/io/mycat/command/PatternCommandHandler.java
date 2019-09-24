package io.mycat.command;

import io.mycat.config.ConfigFile;
import io.mycat.config.ConfigurableRoot;
import io.mycat.config.pattern.PatternRootConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;

public class PatternCommandHandler extends AbstractCommandHandler{

    @Override
    public void initRuntime(MycatSession session, ProxyRuntime runtime) {
        PatternRootConfig config = runtime.getConfig(ConfigFile.PATTERN);
        String defaultSchema = config.getDefaultSchema();
    }

    @Override
    public void handleQuery(byte[] sql, MycatSession session) {

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