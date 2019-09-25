package io.mycat.lib;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.InstructionSet;
import io.mycat.proxy.session.MycatSession;

public class BaseLib implements InstructionSet {
    public static Response useSchemaThenResponseOk(String schema) {
        return new Response() {
            @Override
            public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                matcher.getTableCollector().useSchema(schema);
                session.writeOkEndPacket();
            }
        };
    }
    public static Response cacheLocalFileThenResponse(String fileName) {
        return ConstLib.responseOk;
    }
    public static Response responseOk() {
        return ConstLib.responseOk;
    }
    public static Response useSchemaThenResponseOk() {
        return ConstLib.responseOk;
    }
    public static class ConstLib {
        public final static Response responseOk = (session, matcher) -> session.writeOkEndPacket();
    }
}