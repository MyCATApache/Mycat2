package io.mycat.row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class UnsafeRow {
    //////////////////////////////////////////////////////////////////////////////
    // Private fields and methods
    //////////////////////////////////////////////////////////////////////////////

    private Object baseObject;
    private long baseOffset;

    /**
     * The number of fields in this row, used for calculating the bitset width (and in assertions)
     */
    private int numFields;

    /**
     * The size of this row's backing data, in bytes)
     */
    private int sizeInBytes;

    /**
     * The width of the null tracking bit set, in bytes
     */
    private int bitSetWidthInBytes;

    private long getFieldOffset(int ordinal) {
        return baseOffset + bitSetWidthInBytes + ordinal * 8L;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < numFields : "index (" + index + ") should < " + numFields;
    }

    //////////////////////////////////////////////////////////////////////////////
    // Public methods
    //////////////////////////////////////////////////////////////////////////////

    /**
     * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
     * since the value returned by this constructor is equivalent to a null pointer.
     *
     * @param numFields the number of fields in this row
     */
    public UnsafeRow(int numFields) {
        this.numFields = numFields;
        this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    }

    // for serializer
    public UnsafeRow() {
    }

    public Object getBaseObject() {
        return baseObject;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }


    public int numFields() {
        return numFields;
    }

    /**
     * Update this UnsafeRow to point to different backing data.
     *
     * @param baseObject  the base object
     * @param baseOffset  the offset within the base object
     * @param sizeInBytes the size of this row's backing data, in bytes
     */
    public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
        assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
        assert sizeInBytes % 8 == 0 : "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8";
        this.baseObject = baseObject;
        this.baseOffset = baseOffset;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * Update this UnsafeRow to point to the underlying byte array.
     *
     * @param buf         byte array to point to
     * @param sizeInBytes the number of bytes valid in the byte array
     */
    public void pointTo(byte[] buf, int sizeInBytes) {
        pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    }

    public void setTotalSize(int sizeInBytes) {
        assert sizeInBytes % 8 == 0 : "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8";
        this.sizeInBytes = sizeInBytes;
    }

    public void setNotNullAt(int i) {
        assertIndexIsValid(i);
        BitSetMethods.unset(baseObject, baseOffset, i);
    }


    public void setNullAt(int i) {
        assertIndexIsValid(i);
        BitSetMethods.set(baseObject, baseOffset, i);
        // To preserve row equality, zero out the value when setting the column to null.
        // Since this row does not currently support updates to variable-length values, we don't
        // have to worry about zeroing out that data.
        Platform.putLong(baseObject, getFieldOffset(i), 0);
    }


    public void setInt(int ordinal, int value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putInt(baseObject, getFieldOffset(ordinal), value);
    }


    public void setLong(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putLong(baseObject, getFieldOffset(ordinal), value);
    }


    public void setDouble(int ordinal, double value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
    }


    public void setBoolean(int ordinal, boolean value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putBoolean(baseObject, getFieldOffset(ordinal), value);
    }


    public void setShort(int ordinal, short value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putShort(baseObject, getFieldOffset(ordinal), value);
    }


    public void setByte(int ordinal, byte value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putByte(baseObject, getFieldOffset(ordinal), value);
    }


    public void setFloat(int ordinal, float value) {
        assertIndexIsValid(ordinal);
        setNotNullAt(ordinal);
        Platform.putFloat(baseObject, getFieldOffset(ordinal), value);
    }


    public boolean isNullAt(int ordinal) {
        assertIndexIsValid(ordinal);
        return BitSetMethods.isSet(baseObject, baseOffset, ordinal);
    }


    public boolean getBoolean(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getBoolean(baseObject, getFieldOffset(ordinal));
    }


    public byte getByte(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getByte(baseObject, getFieldOffset(ordinal));
    }


    public short getShort(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getShort(baseObject, getFieldOffset(ordinal));
    }


    public int getInt(int ordinal) {
        assertIndexIsValid(ordinal);
        long fieldOffset = getFieldOffset(ordinal);
        return Platform.getInt(baseObject, fieldOffset);
    }


    public long getLong(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getLong(baseObject, getFieldOffset(ordinal));
    }


    public float getFloat(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getFloat(baseObject, getFieldOffset(ordinal));
    }


    public double getDouble(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getDouble(baseObject, getFieldOffset(ordinal));
    }


    public UTF8String getUTF8String(int ordinal) {
        if (isNullAt(ordinal)) return null;
        final long offsetAndSize = getLong(ordinal);
        final int offset = (int) (offsetAndSize >> 32);
        final int size = (int) offsetAndSize;
        return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
    }


    public byte[] getBinary(int ordinal) {
        if (isNullAt(ordinal)) {
            return null;
        } else {
            final long offsetAndSize = getLong(ordinal);
            final int offset = (int) (offsetAndSize >> 32);
            final int size = (int) offsetAndSize;
            final byte[] bytes = new byte[size];
            Platform.copyMemory(
                    baseObject,
                    baseOffset + offset,
                    bytes,
                    Platform.BYTE_ARRAY_OFFSET,
                    size
            );
            return bytes;
        }
    }


    public CalendarInterval getInterval(int ordinal) {
        if (isNullAt(ordinal)) {
            return null;
        } else {
            final long offsetAndSize = getLong(ordinal);
            final int offset = (int) (offsetAndSize >> 32);
            final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
            final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
            return new CalendarInterval(months, microseconds);
        }
    }


    public UnsafeRow getStruct(int ordinal, int numFields) {
        if (isNullAt(ordinal)) {
            return null;
        } else {
            final long offsetAndSize = getLong(ordinal);
            final int offset = (int) (offsetAndSize >> 32);
            final int size = (int) offsetAndSize;
            final UnsafeRow row = new UnsafeRow(numFields);
            row.pointTo(baseObject, baseOffset + offset, size);
            return row;
        }
    }


    public UnsafeArrayData getArray(int ordinal) {
        if (isNullAt(ordinal)) {
            return null;
        } else {
            final long offsetAndSize = getLong(ordinal);
            final int offset = (int) (offsetAndSize >> 32);
            final int size = (int) offsetAndSize;
            final UnsafeArrayData array = new UnsafeArrayData();
            array.pointTo(baseObject, baseOffset + offset, size);
            return array;
        }
    }


    /**
     * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
     * byte array rather than referencing data stored in a data page.
     */

    public UnsafeRow copy() {
        UnsafeRow rowCopy = new UnsafeRow(numFields);
        final byte[] rowDataCopy = new byte[sizeInBytes];
        Platform.copyMemory(
                baseObject,
                baseOffset,
                rowDataCopy,
                Platform.BYTE_ARRAY_OFFSET,
                sizeInBytes
        );
        rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
        return rowCopy;
    }

    /**
     * Creates an empty UnsafeRow from a byte array with specified numBytes and numFields.
     * The returned row is invalid until we call copyFrom on it.
     */
    public static UnsafeRow createFromByteArray(int numBytes, int numFields) {
        final UnsafeRow row = new UnsafeRow(numFields);
        row.pointTo(new byte[numBytes], numBytes);
        return row;
    }

    /**
     * Copies the input UnsafeRow to this UnsafeRow, and resize the underlying byte[] when the
     * input row is larger than this row.
     */
    public void copyFrom(UnsafeRow row) {
        // copyFrom is only available for UnsafeRow created from byte array.
        assert (baseObject instanceof byte[]) && baseOffset == Platform.BYTE_ARRAY_OFFSET;
        if (row.sizeInBytes > this.sizeInBytes) {
            // resize the underlying byte[] if it's not large enough.
            this.baseObject = new byte[row.sizeInBytes];
        }
        Platform.copyMemory(
                row.baseObject, row.baseOffset, this.baseObject, this.baseOffset, row.sizeInBytes);
        // update the sizeInBytes.
        this.sizeInBytes = row.sizeInBytes;
    }

    /**
     * Write this UnsafeRow's underlying bytes to the given OutputStream.
     *
     * @param out         the stream to write to.
     * @param writeBuffer a byte array for buffering chunks of off-heap data while writing to the
     *                    output stream. If this row is backed by an on-heap byte array, then this
     *                    buffer will not be used and may be null.
     */
    public void writeToStream(OutputStream out, byte[] writeBuffer) throws IOException {
        if (baseObject instanceof byte[]) {
            int offsetInByteArray = (int) (baseOffset - Platform.BYTE_ARRAY_OFFSET);
            out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
        } else {
            int dataRemaining = sizeInBytes;
            long rowReadPosition = baseOffset;
            while (dataRemaining > 0) {
                int toTransfer = Math.min(writeBuffer.length, dataRemaining);
                Platform.copyMemory(
                        baseObject, rowReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
                out.write(writeBuffer, 0, toTransfer);
                rowReadPosition += toTransfer;
                dataRemaining -= toTransfer;
            }
        }
    }


    public int hashCode() {
        return Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42);
    }


    public boolean equals(Object other) {
        if (other instanceof UnsafeRow) {
            UnsafeRow o = (UnsafeRow) other;
            return (sizeInBytes == o.sizeInBytes) &&
                    ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
                            sizeInBytes);
        }
        return false;
    }

    /**
     * Returns the underlying bytes for this UnsafeRow.
     */
    public byte[] getBytes() {
        return UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
    }

    // This is for debugging

    public String toString() {
        StringBuilder build = new StringBuilder("[");
        for (int i = 0; i < sizeInBytes; i += 8) {
            if (i != 0) build.append(',');
            build.append(java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)));
        }
        build.append(']');
        return build.toString();
    }


    public boolean anyNull() {
        return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes / 8);
    }

    /**
     * Writes the content of this row into a memory address, identified by an object and an offset.
     * The target memory address must already been allocated, and have enough space to hold all the
     * bytes in this string.
     */
    public void writeToMemory(Object target, long targetOffset) {
        Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
    }

    public void writeTo(ByteBuffer buffer) {
        assert (buffer.hasArray());
        byte[] target = buffer.array();
        int offset = buffer.arrayOffset();
        int pos = buffer.position();
        writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
        buffer.position(pos + sizeInBytes);
    }

    /**
     * Write the bytes of var-length field into ByteBuffer
     * <p>
     * Note: only work with HeapByteBuffer
     */
    public void writeFieldTo(int ordinal, ByteBuffer buffer) {
        final long offsetAndSize = getLong(ordinal);
        final int offset = (int) (offsetAndSize >> 32);
        final int size = (int) offsetAndSize;

        buffer.putInt(size);
        int pos = buffer.position();
        buffer.position(pos + size);
        Platform.copyMemory(
                baseObject,
                baseOffset + offset,
                buffer.array(),
                Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + pos,
                size);
    }

    public void pointTo(Object baseObject, int sizeInBytes) {
        pointTo(baseObject, 0, sizeInBytes);
    }

    public static int calculateBitSetWidthInBytes(int numFields) {
        return ((numFields + 63) / 64) * 8;
    }
}
