package io.mycat.rpc.cs;

import io.mycat.rpc.RpcSocket;


/**
 * The type Rpc server session handler.
 */
public abstract class RpcServerSessionHandler {

  /**
   * The Last active time.
   */
  long lastActiveTime;

  /**
   * On revc.
   *
   * @param data the data
   * @param worker the worker
   */
  public abstract void onRevc(byte[] data, RpcSocket worker);

  /**
   * Gets last active time.
   *
   * @return the last active time
   */
  public long getLastActiveTime() {
    return lastActiveTime;
  }

  /**
   * Clear.
   */
  public abstract void clear();

  /**
   * On update active time.
   *
   * @param lastActiveTime the last active time
   */
  public void onUpdateActiveTime(long lastActiveTime) {
    this.lastActiveTime = lastActiveTime;
  }

}