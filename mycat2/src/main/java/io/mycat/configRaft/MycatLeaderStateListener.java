package io.mycat.configRaft;

import java.util.Map;

public interface MycatLeaderStateListener {
    /**
     * Called when current node becomes leader
     */
    void onLeaderStart(final long leaderTerm);

    /**
     * Called when current node loses leadership.
     */
    void onLeaderStop(final long leaderTerm);

    void onSet(String key, String value, Map<String,String> map);
}
