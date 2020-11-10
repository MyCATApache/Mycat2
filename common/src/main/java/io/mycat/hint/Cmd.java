package io.mycat.hint;

enum Cmd {
        showSchemas("showSchemas"),
        showTables("showTables"),
        showClusters("showClusters"),
        showDatasources("showDatasources"),
        showHeartbeats("showHeartbeats"),
        showHeartbeatStatus("showHeartbeatStatus"),
        showReactors("showReactors"),
        showThreadPools("showThreadPools"),
        showNativeBackends("showNativeBackends"),
        showConnections("showConnections"),
        showSchedules("showSchedules"),
        ;
        private final String text;

        Cmd(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }
    }