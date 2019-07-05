package io.mycat.sequenceModifier;

public interface ModifyCallback {

  void onSuccessCallback(String sql);

  void onException(Exception e);
}