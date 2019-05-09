/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.packet;

/**
 * copy form com.mysql.cj.protocol.a
 */
public interface PacketSplitter {
    // static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;

    default void init(int totalSize) {
        setTotalSizeInPacketSplitter(totalSize);
        setPacketLenInPacketSplitter(0);
        setOffsetInPacketSplitter(0);
    }
    public static int caculWholePacketSize(int payloadLen){
      return   (payloadLen / (PacketSplitter.MAX_PACKET_SIZE)+1) * 4;
    }
    default boolean nextPacketInPacketSplitter() {
        setOffsetInPacketSplitter(getOffsetInPacketSplitter() + getPacketLenInPacketSplitter());
        // need a zero-len packet if final packet len is MAX_PACKET_SIZE
        if (getPacketLenInPacketSplitter() == MAX_PACKET_SIZE && getOffsetInPacketSplitter() == getTotalSizeInPacketSplitter()) {
            setPacketLenInPacketSplitter(0);
            return true;
        }

        // allow empty packets
        if (getTotalSizeInPacketSplitter() == 0) {
            setTotalSizeInPacketSplitter(-1); // to return `false' next iteration
            return true;
        }

        int currentPacketLen;
        int offset = getOffsetInPacketSplitter();
        int totalSize = getTotalSizeInPacketSplitter();
        currentPacketLen = getTotalSizeInPacketSplitter() - offset;
        if (currentPacketLen > MAX_PACKET_SIZE) {
            setPacketLenInPacketSplitter(MAX_PACKET_SIZE);
        } else {
            setPacketLenInPacketSplitter(currentPacketLen);
        }
        return offset < totalSize;
    }

    public int getTotalSizeInPacketSplitter();

    public void setTotalSizeInPacketSplitter(int totalSize);

    public int getPacketLenInPacketSplitter();

    public void setPacketLenInPacketSplitter(int currentPacketLen);

    public void setOffsetInPacketSplitter(int offset);

    public int getOffsetInPacketSplitter();

}
