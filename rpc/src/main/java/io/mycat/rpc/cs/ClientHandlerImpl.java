package io.mycat.rpc.cs;

public class ClientHandlerImpl extends ClientHandler {

  @Override
  public byte[] getSendMsg() {
    return ("" + seqId).getBytes();
  }

  @Override
  public void onHasSendData() {

  }


  @Override
  public boolean onRevc(byte[] data) {
    String s = new String(data);
    System.out
        .println(s);
    return false;
  }

  @Override
  public void onWaitForTimeout() {

  }

  @Override
  public void onWaitForPollErr() {

  }

  @Override
  public void onWaitForPendingErr() {

  }

  @Override
  public long getTimeout() {
    return 100000000;
  }

  @Override
  public void onBeforeSendDataError() {

  }
}