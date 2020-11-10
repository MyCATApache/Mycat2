package io.mycat.hint;

public  class ShowThreadPoolHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showThreadPools";
        }
    }
