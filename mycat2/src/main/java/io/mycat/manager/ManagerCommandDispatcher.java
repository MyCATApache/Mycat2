package io.mycat.manager;

import io.mycat.DefaultCommandHandler;
import io.mycat.ReceiverImpl;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collection;

public class ManagerCommandDispatcher extends DefaultCommandHandler {
    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        ReceiverImpl receiver = new ReceiverImpl(session);
        String original = new String(bytes);
        original = original.trim();
        original = original.endsWith(";")?original.substring(0,original.length()-1):original;
        String[] s = original.split(" ");
        if (s.length == 2 && "show".equalsIgnoreCase(s[0].trim())) {
            String s1 = s[1].trim().toLowerCase();
            switch (s1){
               case  "@@datasource":{
                   ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
                   resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
                   resultSetBuilder.addColumnInfo("ACTIVE", JDBCType.VARCHAR);
                   Collection<PhysicsInstance> values =
                           ReplicaSelectorRuntime.INSTANCE.getPhysicsInstanceMap().values();
                   for (PhysicsInstance value : values) {
                       resultSetBuilder.addObjectRowPayload(
                               Arrays.asList(value.getName(),value.getSessionCounter()));
                   }
                   receiver.sendResultSet(()->resultSetBuilder.build());
                   return;
               }
            }
        }
//        session.writeOkEndPacket();
        super.handleQuery(bytes, session);
    }
}