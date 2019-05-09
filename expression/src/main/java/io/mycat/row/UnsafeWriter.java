package io.mycat.row;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public abstract class UnsafeWriter {
  // Keep internal buffer holder
  protected final BufferHolder holder;

  // The offset of the global buffer where we start to write this structure.
  protected int startingOffset;

  protected UnsafeWriter(BufferHolder holder) {
    this.holder = holder;
  }

  /**
   * Accessor methods are delegated from BufferHolder class
   */
  public final BufferHolder getBufferHolder() {
    return holder;
  }

  public final byte[] getBuffer() {
    return holder.getBuffer();
  }

  public final void reset() {
    holder.reset();
  }

  public final int totalSize() {
    return holder.totalSize();
  }

  public final void grow(int neededSize) {
    holder.grow(neededSize);
  }

  public final int cursor() {
    return holder.getCursor();
  }

  public final void increaseCursor(int val) {
    holder.increaseCursor(val);
  }

  public final void setOffsetAndSizeFromPreviousCursor(int ordinal, int previousCursor) {
    setOffsetAndSize(ordinal, previousCursor, cursor() - previousCursor);
  }

  protected void setOffsetAndSize(int ordinal, int size) {
    setOffsetAndSize(ordinal, cursor(), size);
  }

  protected void setOffsetAndSize(int ordinal, int currentCursor, int size) {
    final long relativeOffset = currentCursor - startingOffset;
    final long offsetAndSize = (relativeOffset << 32) | (long)size;

    write(ordinal, offsetAndSize);
  }

  protected final void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(getBuffer(), cursor() + ((numBytes >> 3) << 3), 0L);
    }
  }

  public abstract void setNull1Bytes(int ordinal);
  public abstract void setNull2Bytes(int ordinal);
  public abstract void setNull4Bytes(int ordinal);
  public abstract void setNull8Bytes(int ordinal);

  public abstract void write(int ordinal, boolean value);
  public abstract void write(int ordinal, byte value);
  public abstract void write(int ordinal, short value);
  public abstract void write(int ordinal, int value);
  public abstract void write(int ordinal, long value);
  public abstract void write(int ordinal, float value);
  public abstract void write(int ordinal, double value);

  public final void write(int ordinal, UTF8String input) {
    writeUnalignedBytes(ordinal, input.getBaseObject(), input.getBaseOffset(), input.numBytes());
  }

  public final void write(int ordinal, byte[] input) {
    write(ordinal, input, 0, input.length);
  }

  public final void write(int ordinal, byte[] input, int offset, int numBytes) {
    writeUnalignedBytes(ordinal, input, Platform.BYTE_ARRAY_OFFSET + offset, numBytes);
  }

  private void writeUnalignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    grow(roundedSize);
    zeroOutPaddingBytes(numBytes);
    int cursor = cursor();
    Platform.copyMemory(baseObject, baseOffset, getBuffer(),cursor, numBytes);
    setOffsetAndSize(ordinal, numBytes);
    increaseCursor(roundedSize);
  }

  public final void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(getBuffer(), cursor(), input.months);
    Platform.putLong(getBuffer(), cursor() + 8, input.microseconds);

    setOffsetAndSize(ordinal, 16);

    // move the cursor forward.
    increaseCursor(16);
  }

  public final void write(int ordinal, UnsafeRow row) {
    writeAlignedBytes(ordinal, row.getBaseObject(), row.getBaseOffset(), row.getSizeInBytes());
  }

  private void writeAlignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    grow(numBytes);
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    setOffsetAndSize(ordinal, numBytes);
    increaseCursor(numBytes);
  }

  protected final void writeBoolean(long offset, boolean value) {
    Platform.putBoolean(getBuffer(), offset, value);
  }

  protected final void writeByte(long offset, byte value) {
    Platform.putByte(getBuffer(), offset, value);
  }

  protected final void writeShort(long offset, short value) {
    Platform.putShort(getBuffer(), offset, value);
  }

  protected final void writeInt(long offset, int value) {
    Platform.putInt(getBuffer(), offset, value);
  }

  protected final void writeLong(long offset, long value) {
    Platform.putLong(getBuffer(), offset, value);
  }

  protected final void writeFloat(long offset, float value) {
    Platform.putFloat(getBuffer(), offset, value);
  }

  protected final void writeDouble(long offset, double value) {
    Platform.putDouble(getBuffer(), offset, value);
  }
}