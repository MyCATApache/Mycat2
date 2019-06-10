package io.mycat.rpc.cs;

public abstract class ClientHandler {
 protected int seqId;

 abstract byte[] getSendMsg();

  abstract  void onHasSendData();

  public void setSeqId(int id) {
    this.seqId = id;
  }

  abstract public boolean onRevc(byte[] data);

  abstract  public void onWaitForTimeout();

  abstract public void onWaitForPollErr();

  abstract public void onWaitForPendingErr();

  int getSeqId() {
    return seqId;
  }

  abstract  long getTimeout();

  abstract  void onBeforeSendDataError();
}