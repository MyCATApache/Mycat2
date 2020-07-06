/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is open software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.bindthread.BindThreadKey;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.plug.PlugRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author cjw
 **/
public class GRuntimeTest {

    public static void main(String[] args) throws Exception {
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(MycatCore.class);
        MycatConfig mycatConfig = bootConfig.currentConfig();
        PlugRuntime.INSTANCE.load(mycatConfig);
        JdbcRuntime.INSTANCE.load(mycatConfig);
        ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        for (int i = 0; i < 1000; i++) {
            BindThreadKey id = id();
//            JdbcRuntime.INSTANCE.run(id, new GProcess() {
//
//                @Override
//                public void accept(BindThreadKey key) {
//
//                }
//
//                @Override
//                public void accept(BindThreadKey key, TransactionSession session) {
//
//                }
//
//                @Override
//                public void accept(BindThreadKey key, BindThread context) {
//                    session.begin();
//                    DefaultConnection defaultDs = session.getConnection("defaultDs");
//                    JdbcRowBaseIterator jdbcRowBaseIterator = defaultDs.executeQuery("select 1");
//                    List<Map<String, Object>> resultSetMap = jdbcRowBaseIterator.getResultSetMap();
//                    DefaultConnection c2 = session.getConnection("defaultDs2");
//                    List<Map<String, Object>> resultSetMap1 = c2.executeQuery("select 1").getResultSetMap();
//                    session.commit();
//                    System.out.println(resultSetMap1);
//                    countDownLatch.countDown();
//                    System.out.println("-----------------" + countDownLatch);
//                }
//
//                @Override
//                public void finallyAccept(BindThreadKey key, BindThread context) {
//
//                }
//
//                @Override
//                public void onException(BindThreadKey key, Exception e) {
//                    System.out.println(e);
//                }
//            });
        }
        countDownLatch.await();
        System.out.println("----------------------end-------------------------");
    }

   final static AtomicInteger COUNTER = new AtomicInteger();
    private static BindThreadKey id() {
        return new BindThreadKey() {
            final int id = COUNTER.incrementAndGet();

            @Override
            public int hashCode() {
                return id;
            }

            @Override
            public boolean equals(Object obj) {
                return this == obj;
            }

            @Override
            public boolean isRunning() {
                return true;
            }

            @Override
            public boolean continueBindThreadIfTransactionNeed() {
                return false;
            }

        };
    }
}