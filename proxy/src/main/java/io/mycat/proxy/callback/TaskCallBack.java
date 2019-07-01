package io.mycat.proxy.callback;

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;

/**
 * @author jamie12221
 *  date 2019-05-21 01:21
 **/
public interface TaskCallBack<T extends TaskCallBack> {

  default Exception toExpection(ErrorPacketImpl errorPacket) {
    byte[] errorMessage = errorPacket.getErrorMessage();
    return new MycatException(new String(errorMessage));
  }
}
