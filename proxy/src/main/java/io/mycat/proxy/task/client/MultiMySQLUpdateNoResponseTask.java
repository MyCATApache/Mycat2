package io.mycat.proxy.task.client;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.util.Collection;

/**
 * @author jamie12221
 * @date 2019-05-17 12:22
 **/
public class MultiMySQLUpdateNoResponseTask extends MultiMySQLUpdateTask {

  public MultiMySQLUpdateNoResponseTask(MycatSession mycat, byte[] packetData,
      Collection<MySQLDataNode> dataNodeList,
      AsyncTaskCallBack<MycatSession> finalCallBack) {
    super(mycat, packetData, dataNodeList, finalCallBack);
  }

  @Override
  public void onWriteFinished(MySQLClientSession mysql) throws IOException {
    super.clearAndFinished(mysql, true, mysql.getLastMessage());
  }
}
