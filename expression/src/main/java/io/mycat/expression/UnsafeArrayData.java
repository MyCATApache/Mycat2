package io.mycat.expression;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public final class UnsafeArrayData  {
  public static int calculateHeaderPortionInBytes(int numFields) {
    return (int)calculateHeaderPortionInBytes((long)numFields);
  }

  public static long calculateHeaderPortionInBytes(long numFields) {
    return 8 + ((numFields + 63)/ 64) * 8;
  }

  public static long calculateSizeOfUnderlyingByteArray(long numFields, int elementSize) {
    long size = UnsafeArrayData.calculateHeaderPortionInBytes(numFields) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord(numFields * elementSize);
    return size;
  }

  private Object baseObject;
  private long baseOffset;

  // The number of elements in this array
  private int numElements;

  // The size of this array's backing data, in bytes.
  // The 8-bytes header of `numElements` is also included.
  private int sizeInBytes;

  /** The position to start storing array elements, */
  private long elementOffset;

  private long getElementOffset(int ordinal, int elementSize) {
    return elementOffset + ordinal * (long)elementSize;
  }

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  private void assertIndexIsValid(int ordinal) {
    assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
    assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
  }

  public Object[] array() {
    throw new UnsupportedOperationException("Not supported on UnsafeArrayData.");
  }

  /**
   * Construct a new UnsafeArrayData. The resulting UnsafeArrayData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeArrayData() { }

  public int numElements() { return numElements; }

  /**
   * Update this UnsafeArrayData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this array's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the number of elements from the first 8 bytes.
    final long numElements = Platform.getLong(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    assert numElements <= Integer.MAX_VALUE :
      "numElements (" + numElements + ") should <= Integer.MAX_VALUE";

    this.numElements = (int)numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
    this.elementOffset = baseOffset + calculateHeaderPortionInBytes(this.numElements);
  }


  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset + 8, ordinal);
  }



  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getElementOffset(ordinal, 1));
  }


  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getElementOffset(ordinal, 1));
  }


  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getElementOffset(ordinal, 2));
  }


  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getElementOffset(ordinal, 4));
  }


  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getElementOffset(ordinal, 8));
  }


  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getElementOffset(ordinal, 4));
  }


  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getElementOffset(ordinal, 8));
  }



  public UTF8String getUTF8String(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }


  public byte[] getBinary(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final byte[] bytes = new byte[size];
    Platform.copyMemory(baseObject, baseOffset + offset, bytes, Platform.BYTE_ARRAY_OFFSET, size);
    return bytes;
  }


  public CalendarInterval getInterval(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
    final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
    return new CalendarInterval(months, microseconds);
  }


  public UnsafeRow getStruct(int ordinal, int numFields) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(baseObject, baseOffset + offset, size);
    return row;
  }


  public UnsafeArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeArrayData array = new UnsafeArrayData();
    array.pointTo(baseObject, baseOffset + offset, size);
    return array;
  }


  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }


  public void setNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(baseObject, baseOffset + 8, ordinal);

    /* we assume the corresponding column was already 0 or
       will be set to 0 later by the caller side */
  }


  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    Platform.putBoolean(baseObject, getElementOffset(ordinal, 1), value);
  }


  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    Platform.putByte(baseObject, getElementOffset(ordinal, 1), value);
  }


  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    Platform.putShort(baseObject, getElementOffset(ordinal, 2), value);
  }


  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    Platform.putInt(baseObject, getElementOffset(ordinal, 4), value);
  }


  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    Platform.putLong(baseObject, getElementOffset(ordinal, 8), value);
  }


  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    Platform.putFloat(baseObject, getElementOffset(ordinal, 4), value);
  }


  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    Platform.putDouble(baseObject, getElementOffset(ordinal, 8), value);
  }

  // This `hashCode` computation could consume much processor time for large data.
  // If the computation becomes a bottleneck, we can use a light-weight logic; the first fixed bytes
  // are used to compute `hashCode` (See `Vector.hashCode`).
  // The same issue exists in `UnsafeRow.hashCode`.

  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(baseObject, baseOffset, sizeInBytes, 42);
  }


  public boolean equals(Object other) {
    if (other instanceof UnsafeArrayData) {
      UnsafeArrayData o = (UnsafeArrayData) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }


  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = new UnsafeArrayData();
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return arrayCopy;
  }


  public boolean[] toBooleanArray() {
    boolean[] values = new boolean[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.BOOLEAN_ARRAY_OFFSET, numElements);
    return values;
  }


  public byte[] toByteArray() {
    byte[] values = new byte[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.BYTE_ARRAY_OFFSET, numElements);
    return values;
  }


  public short[] toShortArray() {
    short[] values = new short[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.SHORT_ARRAY_OFFSET, numElements * 2L);
    return values;
  }


  public int[] toIntArray() {
    int[] values = new int[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.INT_ARRAY_OFFSET, numElements * 4L);
    return values;
  }


  public long[] toLongArray() {
    long[] values = new long[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.LONG_ARRAY_OFFSET, numElements * 8L);
    return values;
  }


  public float[] toFloatArray() {
    float[] values = new float[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.FLOAT_ARRAY_OFFSET, numElements * 4L);
    return values;
  }


  public double[] toDoubleArray() {
    double[] values = new double[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.DOUBLE_ARRAY_OFFSET, numElements * 8L);
    return values;
  }

  public static UnsafeArrayData fromPrimitiveArray(
       Object arr, int offset, int length, int elementSize) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = (long)elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
        "it's too big.");
    }

    final long[] data = new long[(int)totalSizeInLongs];

    Platform.putLong(data, Platform.LONG_ARRAY_OFFSET, length);
    if (arr != null) {
      Platform.copyMemory(arr, offset, data,
        Platform.LONG_ARRAY_OFFSET + headerInBytes, valueRegionInBytes);
    }

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.LONG_ARRAY_OFFSET, (int)totalSizeInLongs * 8);
    return result;
  }

  public static UnsafeArrayData createFreshArray(int length, int elementSize) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = (long)elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
        "it's too big.");
    }

    final long[] data = new long[(int)totalSizeInLongs];

    Platform.putLong(data, Platform.LONG_ARRAY_OFFSET, length);

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.LONG_ARRAY_OFFSET, (int)totalSizeInLongs * 8);
    return result;
  }

  public static boolean shouldUseGenericArrayData(int elementSize, long length) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    return totalSizeInLongs > Integer.MAX_VALUE / 8;
  }

  public static UnsafeArrayData fromPrimitiveArray(boolean[] arr) {
    return fromPrimitiveArray(arr, Platform.BOOLEAN_ARRAY_OFFSET, arr.length, 1);
  }

  public static UnsafeArrayData fromPrimitiveArray(byte[] arr) {
    return fromPrimitiveArray(arr, Platform.BYTE_ARRAY_OFFSET, arr.length, 1);
  }

  public static UnsafeArrayData fromPrimitiveArray(short[] arr) {
    return fromPrimitiveArray(arr, Platform.SHORT_ARRAY_OFFSET, arr.length, 2);
  }

  public static UnsafeArrayData fromPrimitiveArray(int[] arr) {
    return fromPrimitiveArray(arr, Platform.INT_ARRAY_OFFSET, arr.length, 4);
  }

  public static UnsafeArrayData fromPrimitiveArray(long[] arr) {
    return fromPrimitiveArray(arr, Platform.LONG_ARRAY_OFFSET, arr.length, 8);
  }

  public static UnsafeArrayData fromPrimitiveArray(float[] arr) {
    return fromPrimitiveArray(arr, Platform.FLOAT_ARRAY_OFFSET, arr.length, 4);
  }

  public static UnsafeArrayData fromPrimitiveArray(double[] arr) {
    return fromPrimitiveArray(arr, Platform.DOUBLE_ARRAY_OFFSET, arr.length, 8);
  }

}