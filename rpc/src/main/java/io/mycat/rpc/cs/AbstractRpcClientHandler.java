package io.mycat.rpc.cs;

/**
 * The type Abstract rpc client handler.
 */
public abstract class AbstractRpcClientHandler {

  /**
   * Get send msg byte [ ].
   *
   * @return the byte [ ]
   */
  public abstract byte[] getSendMsg();

  /**
   * On has send data.
   */
  protected abstract void onHasSendData();

  /**
   * On revc boolean.
   *
   * @param data the data
   * @return the boolean
   */
  abstract public boolean onRevc(byte[] data);

  /**
   * On wait for response timeout.
   */
  abstract public void onWaitForResponseTimeout();

  /**
   * On wait for response err.
   *
   * @param message the message
   */
  abstract public void onWaitForResponseErr(String message);

  /**
   * On wait for pending err.
   */
  abstract public void onWaitForPendingErr();


  /**
   * Gets timeout.
   *
   * @return the timeout
   */
  public abstract long getTimeout();

  /**
   * On before send data error.
   */
  abstract void onBeforeSendDataError();
}