package io.mycat.memory;

public class TooLargePageException extends RuntimeException {
  TooLargePageException(long size) {
    super("Cannot allocate a page of " + size + " bytes.");
  }
}