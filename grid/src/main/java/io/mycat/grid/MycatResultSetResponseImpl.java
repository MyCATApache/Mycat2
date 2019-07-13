package io.mycat.grid;

import io.mycat.proxy.packet.ColumnDefPacketImpl;

public class MycatResultSetResponseImpl implements MycatResultSetResponse {

  @Override
  public int getColumnCount() {
    return 0;
  }

  @Override
  public ColumnDefPacketImpl[] getColumnDefPayloads() {
    return new ColumnDefPacketImpl[0];
  }

  @Override
  public Iterable<byte[][]> getRows() {
    return null;
  }
}