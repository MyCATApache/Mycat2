package io.mycat.mycat2.cmds.judge;

import io.mycat.mysql.ServerStatus;

public class JudgeUtil {


    public static boolean hasMulitQuery(int serverStatus) {
        return  ServerStatus.statusCheck(serverStatus, ServerStatus.MULIT_QUERY);
    }
    public static boolean hasMoreResult(int serverStatus) {
        return  ServerStatus.statusCheck(serverStatus, ServerStatus.MORE_RESULTS);
    }
    public static boolean hasResult(int serverStatus) {
        return (hasMoreResult(serverStatus) || hasMulitQuery(serverStatus));
    }

    public static boolean hasFatch(int serverStatus) {
        // 检查是否通过fatch执行的语句
        return ServerStatus.statusCheck(serverStatus, ServerStatus.CURSOR_EXISTS);
    }

    public static boolean hasTrans(int serverStatus) {
        // 检查是否通过fatch执行的语句
        boolean trans = ServerStatus.statusCheck(serverStatus, ServerStatus.IN_TRANSACTION);
        return trans;
    }




}
