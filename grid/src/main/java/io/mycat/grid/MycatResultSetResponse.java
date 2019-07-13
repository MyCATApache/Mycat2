package io.mycat.grid;

import io.mycat.proxy.packet.ColumnDefPacketImpl;

public interface MycatResultSetResponse extends MycatResponse {

  default MycatResultSetType getType() {
    return MycatResultSetType.RRESULTSET;
  }

  int getColumnCount();

  ColumnDefPacketImpl[] getColumnDefPayloads();

  Iterable<byte[][]> getRows();

}
