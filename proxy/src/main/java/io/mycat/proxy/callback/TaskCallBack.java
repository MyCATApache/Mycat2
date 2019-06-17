package io.mycat.proxy.callback;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.proxy.packet.ErrorPacketImpl;

/**
 * @author jamie12221
 *  date 2019-05-21 01:21
 **/
public interface TaskCallBack<T extends TaskCallBack> {

  default Exception toExpection(ErrorPacketImpl errorPacket) {
    byte[] errorMessage = errorPacket.getErrorMessage();
    return new MycatExpection(new String(errorMessage));
  }
}
