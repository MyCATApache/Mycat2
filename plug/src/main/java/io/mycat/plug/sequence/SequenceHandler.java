package io.mycat.plug.sequence;

/**
 *
 */
public interface SequenceHandler {

  void nextId(final SequenceCallback callback);
}