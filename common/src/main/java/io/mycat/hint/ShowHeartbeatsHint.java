package io.mycat.hint;

public  class ShowHeartbeatsHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showHeartbeats";
        }
    }
