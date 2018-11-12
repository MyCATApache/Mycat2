package io.mycat.mycat2.cmds.judge;

/**
 * cjw
 * 2947122221@qq.com
 */
public interface MySQLPacketCallback {

    default void onRsColCount(MySQLProxyStateM sm) {

    }

    default void onRsColDef(MySQLProxyStateM sm) {

    }

    default void onRsRow(MySQLProxyStateM sm) {

    }

    default void onRsFinish(MySQLProxyStateM sm) {

    }


    default void onCommandFinished(MySQLProxyStateM sm) {

    }

    default void onServerStatusChanged(MySQLProxyStateM sm, int old, int recent) {

    }

    default void onInteractiveFinished(MySQLProxyStateM sm) {

    }
}
