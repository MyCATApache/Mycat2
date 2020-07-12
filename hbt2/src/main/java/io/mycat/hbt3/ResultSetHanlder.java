package io.mycat.hbt3;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mpp.Row;

public interface ResultSetHanlder {
   void onOk();
   void onMetadata(MycatRowMetaData mycatRowMetaData);
   void onRow(Row row);
   void onError(Throwable e);
}