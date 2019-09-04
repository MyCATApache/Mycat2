package io.mycat.replica;

public enum InstanceType {
  READ(false, true),
  WRITE(true, false),
  READ_WRITE(true, true);

  private boolean writeType;
  private boolean readType;

  InstanceType(boolean writeType, boolean readType) {
    this.writeType = writeType;
    this.readType = readType;
  }

  public boolean isWriteType() {
    return writeType;
  }

  public boolean isReadType() {
    return writeType;
  }
}