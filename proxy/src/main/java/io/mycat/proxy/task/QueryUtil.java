package io.mycat.proxy.task;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLSetOption;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-10 21:57
 **/
public class QueryUtil {

  private final static SetOptionTask SET_OPTION = new SetOptionTask();

  public static void setOption(
      MySQLClientSession mysql, MySQLSetOption setOption,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    SET_OPTION.request(mysql, setOption, callBack);
  }

  private static class SetOptionTask implements ResultSetTask {
    public void request(
        MySQLClientSession mysql, MySQLSetOption setOption,
        AsynTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, setOption, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    public void request(MySQLClientSession mysql, MySQLSetOption setOption,
        MycatReactorThread curThread, AsynTaskCallBack<MySQLClientSession> callBack) {

      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
      MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(7);
      mySQLPacket.writeByte((byte) MySQLCommandType.COM_SET_OPTION);
      mySQLPacket.writeFixInt(2, setOption.getValue());

      try {
        mysql.setCallBack(callBack);
        mysql.switchNioHandler(this);
        mysql.prepareReveiceResponse();
        mysql.writeProxyPacket(mySQLPacket, mysql.setPacketId(0));
      } catch (IOException e) {
        this.clearAndFinished(mysql, false, e.getMessage());
      }
    }

  }
}
