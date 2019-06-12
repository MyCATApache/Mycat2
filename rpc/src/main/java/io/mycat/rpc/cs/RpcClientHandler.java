package io.mycat.rpc.cs;

/**
 * The type Rpc client handler.
 */
public abstract class RpcClientHandler extends AbstractRpcClientHandler {

  @Override
  protected void onHasSendData() {

  }

  @Override
  public void onWaitForResponseTimeout() {
    onResponseError("timeout");
  }

  @Override
  public void onWaitForResponseErr(String message) {
    onResponseError(message);
  }

  @Override
  public void onWaitForPendingErr() {
    onRetry();
  }


  @Override
  void onBeforeSendDataError() {
    onRetry();
  }

  /**
   * On retry.
   */
  public abstract void onRetry();

  /**
   * On response error.
   *
   * @param message the message
   */
  public abstract void onResponseError(String message);
}