/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.a;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;

/**
 * PacketPayload is the content of a full single packet (independent from
 * on-wire splitting) communicated with the server. We can manipulate the
 * packet's underlying buffer when sending commands with writeInteger(),
 * writeBytes(), etc. We can check the packet type with isEOFPacket(), etc
 * predicates.
 * 
 * A position is maintained for reading/writing data. A payload length is
 * maintained allowing the PacketPayload to be decoupled from the size of
 * the underlying buffer.
 * 
 */
public class NativePacketPayload implements Message {
    static final int NO_LENGTH_LIMIT = -1;
    public static final long NULL_LENGTH = -1;

    /* Type ids of response packets. */
    public static final short TYPE_ID_ERROR = 0xFF;
    public static final short TYPE_ID_EOF = 0xFE;
    /** It has the same signature as EOF, but may be issued by server only during handshake phase **/
    public static final short TYPE_ID_AUTH_SWITCH = 0xFE;
    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;
    public static final short TYPE_ID_OK = 0;

    private int payloadLength = 0;

    private byte[] byteBuffer;

    private int position = 0;

    static final int MAX_BYTES_TO_DUMP = 1024;

    @Override
    public String toString() {
        int numBytes = this.position <= this.payloadLength ? this.position : this.payloadLength;
        int numBytesToDump = numBytes < MAX_BYTES_TO_DUMP ? numBytes : MAX_BYTES_TO_DUMP;

        this.position = 0;
        String dumped = StringUtils.dumpAsHex(readBytes(StringLengthDataType.STRING_FIXED, numBytesToDump), numBytesToDump);

        if (numBytesToDump < numBytes) {
            return dumped + " ....(packet exceeds max. dump length)";
        }

        return dumped;
    }

    public String toSuperString() {
        return super.toString();
    }

    public NativePacketPayload(byte[] buf) {
        this.byteBuffer = buf;
        this.payloadLength = buf.length;
    }

    public NativePacketPayload(int size) {
        this.byteBuffer = new byte[size];
        this.payloadLength = size;
    }

    public int getCapacity() {
        return this.byteBuffer.length;
    }

    /**
     * Checks that underlying buffer has enough space to store additionalData bytes starting from current position.
     * If buffer size is smaller than required then it is re-allocated with bigger size.
     * 
     * @param additionalData
     *            additional data size in bytes
     */
    public final void ensureCapacity(int additionalData) {
        if ((this.position + additionalData) > this.byteBuffer.length) {
            //
            // Resize, and pad so we can avoid allocing again in the near future
            //
            int newLength = (int) (this.byteBuffer.length * 1.25);

            if (newLength < (this.byteBuffer.length + additionalData)) {
                newLength = this.byteBuffer.length + (int) (additionalData * 1.25);
            }

            if (newLength < this.byteBuffer.length) {
                newLength = this.byteBuffer.length + additionalData;
            }

            byte[] newBytes = new byte[newLength];

            System.arraycopy(this.byteBuffer, 0, newBytes, 0, this.byteBuffer.length);
            this.byteBuffer = newBytes;
        }
    }

    @Override
    public byte[] getByteBuffer() {
        return this.byteBuffer;
    }

    /**
     * Sets the array of bytes to use as a buffer to read from.
     * 
     * @param byteBufferToSet
     *            the array of bytes to use as a buffer
     */
    public void setByteBuffer(byte[] byteBufferToSet) {
        this.byteBuffer = byteBufferToSet;
    }

    /**
     * Get the actual length of payload the buffer contains.
     * It can be smaller than underlying buffer size because it can be reused after a big packet.
     * 
     * @return payload length
     */
    public int getPayloadLength() {
        return this.payloadLength;
    }

    /**
     * Set the actual length of payload written to buffer.
     * It can be smaller or equal to underlying buffer size.
     * 
     * @param bufLengthToSet
     *            length
     */
    public void setPayloadLength(int bufLengthToSet) {
        if (bufLengthToSet > this.byteBuffer.length) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Buffer.0"));
        }
        this.payloadLength = bufLengthToSet;
    }

    /**
     * To be called after write operations to ensure that payloadLength contains
     * the real size of written data.
     */
    private void adjustPayloadLength() {
        if (this.position > this.payloadLength) {
            this.payloadLength = this.position;
        }
    }

    @Override
    public int getPosition() {
        return this.position;
    }

    /**
     * Set the current position to write to/ read from
     * 
     * @param positionToSet
     *            the position (0-based index)
     */
    public void setPosition(int positionToSet) {
        this.position = positionToSet;
    }

    /**
     * Is it a ERROR packet.
     * 
     * @return true if it is a ERROR packet
     */
    public boolean isErrorPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_ERROR;
    }

    /**
     * Is it a EOF packet.
     * See http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
     * 
     * @return true if it is a EOF packet
     */
    public final boolean isEOFPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getPayloadLength() <= 5);
    }

    /**
     * Is it a Protocol::AuthSwitchRequest packet.
     * See http://dev.mysql.com/doc/internals/en/connection-phase-packets.html
     * 
     * @return true if it is a Protocol::AuthSwitchRequest packet
     */
    public final boolean isAuthMethodSwitchRequestPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_AUTH_SWITCH;
    }

    /**
     * Is it an OK packet.
     * See http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
     * 
     * @return true if it is an OK packet
     */
    public final boolean isOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_OK;
    }

    /**
     * Is it an OK packet for ResultSet. Unlike usual 0x00 signature it has 0xfe signature.
     * See http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
     * 
     * @return true if it is an OK packet for ResultSet
     */
    public final boolean isResultSetOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getPayloadLength() < 16777215);
    }

    /**
     * Is it a Protocol::AuthMoreData packet.
     * See http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData
     * 
     * @return true if it is a Protocol::AuthMoreData packet
     */
    public final boolean isAuthMoreData() {
        return ((this.byteBuffer[0] & 0xff) == 1);
    }

    /**
     * Write data according to provided Integer type.
     * 
     * @param type
     *            {@link IntegerDataType}
     * @param l
     *            value
     */
    public void writeInteger(IntegerDataType type, long l) {
        byte[] b;
        switch (type) {
            case INT1:
                ensureCapacity(1);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                break;

            case INT2:
                ensureCapacity(2);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                break;

            case INT3:
                ensureCapacity(3);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                break;

            case INT4:
                ensureCapacity(4);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                break;

            case INT6:
                ensureCapacity(6);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                b[this.position++] = (byte) (l >>> 32);
                b[this.position++] = (byte) (l >>> 40);
                break;

            case INT8:
                ensureCapacity(8);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                b[this.position++] = (byte) (l >>> 32);
                b[this.position++] = (byte) (l >>> 40);
                b[this.position++] = (byte) (l >>> 48);
                b[this.position++] = (byte) (l >>> 56);
                break;

            case INT_LENENC:
                if (l < 251) {
                    ensureCapacity(1);
                    writeInteger(IntegerDataType.INT1, l);

                } else if (l < 65536L) {
                    ensureCapacity(3);
                    writeInteger(IntegerDataType.INT1, 252);
                    writeInteger(IntegerDataType.INT2, l);

                } else if (l < 16777216L) {
                    ensureCapacity(4);
                    writeInteger(IntegerDataType.INT1, 253);
                    writeInteger(IntegerDataType.INT3, l);

                } else {
                    ensureCapacity(9);
                    writeInteger(IntegerDataType.INT1, 254);
                    writeInteger(IntegerDataType.INT8, l);
                }
        }

        adjustPayloadLength();
    }

    /**
     * Read data according to provided Integer type.
     * 
     * @param type
     *            {@link IntegerDataType}
     * @return long
     */
    public final long readInteger(IntegerDataType type) {
        byte[] b = this.byteBuffer;
        switch (type) {
            case INT1:
                return (b[this.position++] & 0xff);

            case INT2:
                return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);

            case INT3:
                return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16);

            case INT4:
                return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24);

            case INT6:
                return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40);

            case INT8:
                return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40)
                        | ((long) (b[this.position++] & 0xff) << 48) | ((long) (b[this.position++] & 0xff) << 56);

            case INT_LENENC:
                int sw = b[this.position++] & 0xff;
                switch (sw) {
                    case 251:
                        return NULL_LENGTH; // represents a NULL in a ProtocolText::ResultsetRow
                    case 252:
                        return readInteger(IntegerDataType.INT2);
                    case 253:
                        return readInteger(IntegerDataType.INT3);
                    case 254:
                        return readInteger(IntegerDataType.INT8);
                    default:
                        return sw;
                }

            default:
                return (b[this.position++] & 0xff);
        }
    }

    /**
     * Write all bytes from given byte array into internal buffer starting with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     */
    public final void writeBytes(StringSelfDataType type, byte[] b) {
        writeBytes(type, b, 0, b.length);
    }

    /**
     * Write all bytes from given byte array into internal buffer starting with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     */
    public final void writeBytes(StringLengthDataType type, byte[] b) {
        writeBytes(type, b, 0, b.length);
    }

    /**
     * Write len bytes from given byte array into internal buffer.
     * Read starts from given offset, write starts with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     * @param offset
     *            starting index of b
     * @param len
     *            number of bytes to be written
     */
    public void writeBytes(StringSelfDataType type, byte[] b, int offset, int len) {
        switch (type) {
            case STRING_EOF:
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                break;

            case STRING_TERM:
                ensureCapacity(len + 1);
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                this.byteBuffer[this.position++] = 0;
                break;

            case STRING_LENENC:
                ensureCapacity(len + 9);
                writeInteger(IntegerDataType.INT_LENENC, len);
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                break;
        }

        adjustPayloadLength();
    }

    /**
     * Write len bytes from given byte array into internal buffer.
     * Read starts from given offset, write starts with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     * @param offset
     *            starting index of b
     * @param len
     *            number of bytes to be written
     */
    public void writeBytes(StringLengthDataType type, byte[] b, int offset, int len) {
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                ensureCapacity(len);
                System.arraycopy(b, offset, this.byteBuffer, this.position, len);
                this.position += len;
                break;
        }

        adjustPayloadLength();
    }

    /**
     * Read bytes from internal buffer starting from current position into the new byte array.
     * The length of data to read depends on {@link StringSelfDataType}.
     * 
     * @param type
     *            {@link StringSelfDataType}
     * @return bytes
     */
    public byte[] readBytes(StringSelfDataType type) {
        byte[] b;
        switch (type) {
            case STRING_TERM:
                int i = this.position;
                while ((i < this.payloadLength) && (this.byteBuffer[i] != 0)) {
                    i++;
                }
                b = readBytes(StringLengthDataType.STRING_FIXED, i - this.position);
                this.position++; // skip terminating byte
                return b;

            case STRING_LENENC:
                long l = readInteger(IntegerDataType.INT_LENENC);
                return l == NULL_LENGTH ? null : (l == 0 ? Constants.EMPTY_BYTE_ARRAY : readBytes(StringLengthDataType.STRING_FIXED, (int) l));

            case STRING_EOF:
                return readBytes(StringLengthDataType.STRING_FIXED, this.payloadLength - this.position);
        }
        return null;
    }

    /**
     * Set position to next value in internal buffer skipping the current value according to {@link StringSelfDataType}.
     * 
     * @param type
     *            {@link StringSelfDataType}
     */
    public void skipBytes(StringSelfDataType type) {
        switch (type) {
            case STRING_TERM:
                while ((this.position < this.payloadLength) && (this.byteBuffer[this.position] != 0)) {
                    this.position++;
                }
                this.position++; // skip terminating byte
                break;

            case STRING_LENENC:
                long len = readInteger(IntegerDataType.INT_LENENC);
                if (len != NULL_LENGTH && len != 0) {
                    this.position += (int) len;
                }
                break;

            case STRING_EOF:
                this.position = this.payloadLength;
                break;
        }
    }

    /**
     * Read len bytes from internal buffer starting from current position into the new byte array.
     * 
     * @param type
     *            {@link StringLengthDataType}
     * @param len
     *            length
     * @return bytes
     */
    public byte[] readBytes(StringLengthDataType type, int len) {
        byte[] b;
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                b = new byte[len];
                System.arraycopy(this.byteBuffer, this.position, b, 0, len);
                this.position += len;
                return b;
        }
        return null;
    }

    /**
     * Read bytes from internal buffer starting from current position decoding them into String using the specified character encoding.
     * The length of data to read depends on {@link StringSelfDataType}.
     * 
     * @param type
     *            {@link StringSelfDataType}
     * @param encoding
     *            if null then platform default encoding is used
     * @return string
     */
    public String readString(StringSelfDataType type, String encoding) {
        String res = null;
        switch (type) {
            case STRING_TERM:
                int i = this.position;
                while ((i < this.payloadLength) && (this.byteBuffer[i] != 0)) {
                    i++;
                }
                res = readString(StringLengthDataType.STRING_FIXED, encoding, i - this.position);
                this.position++; // skip terminating byte
                break;

            case STRING_LENENC:
                long l = readInteger(IntegerDataType.INT_LENENC);
                return l == NULL_LENGTH ? null : (l == 0 ? "" : readString(StringLengthDataType.STRING_FIXED, encoding, (int) l));

            case STRING_EOF:
                return readString(StringLengthDataType.STRING_FIXED, encoding, this.payloadLength - this.position);

        }
        return res;
    }

    /**
     * Read len bytes from internal buffer starting from current position decoding them into String using the specified character encoding.
     * 
     * @param type
     *            {@link StringLengthDataType}
     * @param encoding
     *            if null then platform default encoding is used
     * @param len
     *            length
     * @return string
     */
    public String readString(StringLengthDataType type, String encoding, int len) {
        String res = null;
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                if ((this.position + len) > this.payloadLength) {
                    new Throwable().printStackTrace();
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Buffer.1"));
                }

                res = StringUtils.toString(this.byteBuffer, this.position, len, encoding);
                this.position += len;
                break;

        }
        return res;
    }

    public static String extractSqlFromPacket(String possibleSqlQuery, NativePacketPayload packet, int endOfQueryPacketPosition, int maxQuerySizeToLog) {
        String extractedSql = null;

        if (possibleSqlQuery != null) {
            if (possibleSqlQuery.length() > maxQuerySizeToLog) {
                StringBuilder truncatedQueryBuf = new StringBuilder(possibleSqlQuery.substring(0, maxQuerySizeToLog));
                truncatedQueryBuf.append(Messages.getString("MysqlIO.25"));
                extractedSql = truncatedQueryBuf.toString();
            } else {
                extractedSql = possibleSqlQuery;
            }
        }

        if (extractedSql == null) {
            // This is probably from a client-side prepared statement

            int extractPosition = endOfQueryPacketPosition;

            boolean truncated = false;

            if (endOfQueryPacketPosition > maxQuerySizeToLog) {
                extractPosition = maxQuerySizeToLog;
                truncated = true;
            }

            extractedSql = StringUtils.toString(packet.getByteBuffer(), 1, (extractPosition - 1));

            if (truncated) {
                extractedSql += Messages.getString("MysqlIO.25");
            }
        }

        return extractedSql;
    }

}
