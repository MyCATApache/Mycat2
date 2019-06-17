package io.mycat.command.prepareStatement;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

public class PrepareStmtProxy {

  private HashMap<String, Long> clientStatementId = new HashMap<>();
  private HashMap<Long, ArrayList<PrepareMySQLSessionInfo>> stmtIdMap = new HashMap<>();
  private HashMap<Long, HashMap<Integer, MySQLPayloadWriter>> longDataMap = new HashMap<>();
  private HashMap<String, List<byte[]>> prepareResponse = new HashMap<>();

  private void getPrepareMySQLSession(MycatSession session, MySQLDataNode dataNode, long stmtId,
      SessionCallBack<MySQLClientSession> callBack) {
    ArrayList<PrepareMySQLSessionInfo> items = stmtIdMap.get(stmtId);
    Objects.requireNonNull(items);
    session.switchDataNode(dataNode.getName());

    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();


  }

  public void recordPrepareResponse(String sql, List<byte[]> payloadList) {
    Objects.requireNonNull(payloadList);
    prepareResponse.put(sql, payloadList);
  }

  public Iterator<byte[]> getPrepareResponse(String sql) {
    List<byte[]> bytes = prepareResponse.get(sql);
    Objects.requireNonNull(bytes);
    return bytes.iterator();
  }

  public boolean existPrepareResponse(String sql) {
    return clientStatementId.containsKey(sql);
  }

  public void record(MySQLDatasource datasource,String sql, long statementId, int mysqlSessionId) {
    Long clientStmtId = clientStatementId.get(sql);
    if (clientStmtId == null) {
      clientStatementId.put(sql, statementId);
      stmtIdMap.compute(statementId, (aLong, items) -> {
        if (items == null) {
          items = new ArrayList<>();
        }
        items.add(new PrepareMySQLSessionInfo(datasource,aLong, mysqlSessionId));
        return items;
      });
    } else {
      stmtIdMap.compute(statementId, (aLong, items) -> {
        if (items == null) {
          items = new ArrayList<>();
        }
        items.add(new PrepareMySQLSessionInfo(datasource,clientStmtId, mysqlSessionId));
        return items;
      });
    }
  }

  public void appendLongData(long clientStatementId, int paramId, byte[] data) {
    longDataMap.compute(clientStatementId,
        (aLong, parmaMap) -> {
          if (parmaMap == null) {
            parmaMap = new HashMap<>();
          }
          parmaMap.compute(paramId,
              (integer, writer) -> {
                if (writer == null) {
                  writer = new MySQLPayloadWriter();
                }
                writer.writeBytes(data);
                return writer;
              });
          return parmaMap;
        });
  }

  public void reset(long clientStatementId) {
    longDataMap.remove(clientStatementId);
  }

  public void close(long clientStatementId) {
    String key = null;
    for (Entry<String, Long> entry : this.clientStatementId.entrySet()) {
      if (entry.getValue() == clientStatementId) {
        key = entry.getKey();
        break;
      }
    }
    if (key != null) {
      this.clientStatementId.remove(key);
      this.stmtIdMap.remove(clientStatementId);
      this.longDataMap.remove(clientStatementId);
    }
  }

  public boolean existLongDataPacket(long clientStatementId) {
    HashMap<Integer, MySQLPayloadWriter> map = longDataMap
        .get(clientStatementId);
    return (map != null && !map.isEmpty());
  }

  public byte[] generateAllLongDataPacket(long clientStatementId) {
    HashMap<Integer, MySQLPayloadWriter> map = longDataMap
        .get(clientStatementId);
    MySQLPacketSplitter packetSplitter = new PacketSplitterImpl();
    int packetId = 0;

    int sum = 0;
    for (Entry<Integer, MySQLPayloadWriter> entry : map.entrySet()) {
      sum += MySQLPacketSplitter.caculWholePacketSize(entry.getValue().size());
    }

    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(sum)) {
      for (Entry<Integer, MySQLPayloadWriter> entry : map.entrySet()) {
        Integer key = entry.getKey();
        byte[] data = entry.getValue().toByteArray();
        packetSplitter.init(data.length);
        while (packetSplitter.nextPacketInPacketSplitter()) {
          byte[] payload = MySQLPacketUtil.generateLondData(clientStatementId, key, data);
          byte[] packet = MySQLPacketUtil.generateMySQLPacket(packetId, payload);
          writer.writeBytes(packet);
          ++packetId;
        }
      }
      return writer.toByteArray();
    }
  }

  public Collection<PrepareMySQLSessionInfo> getMySQLSessionInfoByStatementId(long statementId) {
    Collection<PrepareMySQLSessionInfo> res = Collections
        .unmodifiableCollection(this.stmtIdMap.get(statementId));
    return res;
  }
}