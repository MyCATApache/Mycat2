package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;

import java.util.ArrayList;
import java.util.List;

public class ShowTablesTask implements ResultSetTask {
   final List<String> tableNameList = new ArrayList<>();
    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {
        String tableName = mySQLPacket.getLenencString(startPos + 4);
        tableNameList.add(tableName);
    }

    @Override
    public Object getResult() {
        return tableNameList;
    }
}
