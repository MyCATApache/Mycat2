package io.mycat;

import io.mycat.api.collector.RowIterable;
import io.mycat.commands.SQLExecuterWriterHandler;

public class VertxResponse implements Response {

        private VertxSession session;
        SQLExecuterWriterHandler writerHandler;
        public VertxResponse(VertxSession session, int size) {

                this.session = session;
        }

        @Override
        public void sendError(Throwable e) {

        }

        @Override
        public void proxySelectToPrototype(String statement) {

        }

        @Override
        public void proxySelect(String defaultTargetName, String statement) {

        }

        @Override
        public void proxyUpdate(String defaultTargetName, String proxyUpdate) {

        }

        @Override
        public void tryBroadcastShow(String statement) {

        }

        @Override
        public void sendError(String errorMessage, int errorCode) {

        }

        @Override
        public void sendResultSet(RowIterable rowIterable) {

        }



        @Override
        public void rollback() {

        }

        @Override
        public void begin() {

        }

        @Override
        public void commit() {

        }

        @Override
        public void execute(ExplainDetail detail) {

        }

        @Override
        public void sendOk(long lastInsertId, long affectedRow) {

        }

        @Override
        public <T> T unWrapper(Class<T> clazz) {
            return null;
        }
    }