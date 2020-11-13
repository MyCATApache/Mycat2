///**
// * Copyright (C) <2019>  <chen junwen>
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.plug.sequence.sequencefile;
//
//import io.mycat.plug.sequence.sequencefile.SequenceFileHandler.SequenceFile;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.channels.FileChannel.MapMode;
//
//public class SequenceFileImpl implements SequenceFile {
//
//  final MappedByteBuffer buffer;
//  final FileChannel channel;
//
//  public SequenceFileImpl(String path) throws IOException {
//    channel = new RandomAccessFile(path, "rw").getChannel();
//    buffer = channel.map(MapMode.READ_WRITE, 0, 128);
//  }
//
//  @Override
//  public long getCurrentValue() {
//    return buffer.getLong(0);
//  }
//
//  @Override
//  public long setCurrentValue(long currentValue) {
//    buffer.putLong(0, currentValue);
//    return currentValue;
//  }
//
//  @Override
//  public long getMaxValue() {
//    return buffer.getLong(8);
//  }
//
//  @Override
//  public long setMaxValue(long maxValue) {
//    buffer.putLong(0, maxValue);
//    return maxValue;
//  }
//
//  @Override
//  public long getMinValue() {
//    return buffer.getLong(16);
//  }
//
//  @Override
//  public long setMinValue(long minValue) {
//    buffer.putLong(16, minValue);
//    return minValue;
//  }
//
//  @Override
//  public byte[] getHistorySequence() {
//    buffer.position(24);
//    int length = buffer.limit() - buffer.position();
//    byte[] bytes = new byte[length];
//    buffer.get(bytes);
//    return bytes;
//  }
//
//  private void setHistorySequence(byte[] bytes) {
//    buffer.position(24);
//    buffer.put(bytes);
//  }
//
//  @Override
//  public void updateCurrentValue(long currentValue) {
//    buffer.putLong(0, currentValue);
//  }
//
//  @Override
//  public void fetchNextPeriod() throws IOException {
//    long minValue = this.getMinValue();
//    long maxValue = this.getMaxValue();
//    byte[] hs = this.getHistorySequence();
//    StringBuilder sb = new StringBuilder();
//    if (hs.length != 0) {
//      sb.append(",");
//    }
//    sb.append(minValue);
//    sb.append("-");
//    sb.append(maxValue);
//    this.setHistorySequence(sb.toString().getBytes());
//
//    this.setMinValue(maxValue + 1);
//    this.setMaxValue(maxValue - minValue + maxValue + 1);
//    this.setCurrentValue(maxValue);
//
//    try {
//      store(buffer);
//    } catch (Exception e) {
//      channel.close();
//      throw e;
//    }
//  }
//
//  private void store(MappedByteBuffer buffer) {
//    buffer.force();
//  }
//
//}