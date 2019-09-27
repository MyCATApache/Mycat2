package io.mycat.lib;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.InstructionSet;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.session.MycatSession;

public class BaseLibExport implements InstructionSet {
    public static Response useSchemaThenResponseOk(String schema) {
        return Lib.useSchemaThenResponseOk(schema);
    }

    public static Response cacheLocalFileThenResponse(String fileName) {
        return Lib.cacheLocalFileThenResponse(fileName);
    }

    public static Response responseOk() {
        return Lib.responseOk;
    }

    public static Response useSchemaThenResponseOk() {
        return Lib.responseOk;
    }

    public static class Lib {
        public final static Response responseOk = (session, matcher) -> session.writeOkEndPacket();

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
            InserParser inserParser = new InserParser(fileName);
            return new Response() {
                @Override
                public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                    SQLExecuterWriter.writeToMycatSession(session, () -> new TextResultSetResponse(inserParser));
                }
            };
        }
    }
}