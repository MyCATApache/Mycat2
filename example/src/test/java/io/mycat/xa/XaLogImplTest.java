/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.xa;

import cn.mycat.vertx.xa.State;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.JdbcUtils;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.mycat.xa.XaTestSuite.demoConfig;

@net.jcip.annotations.NotThreadSafe
@ExtendWith(VertxExtension.class)
public class XaLogImplTest {
    public static final String DB1 = System.getProperty("db1", "jdbc:mysql://127.0.0.1:3306/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    public static final String DB2 = System.getProperty("db2", "jdbc:mysql://127.0.0.1:3307/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    public static final Map<String, Connection> connectionMap = new HashMap<>();

    private XaLog getDemoRepository() {
        return XaLogImpl.createXaLog(new TestMySQLManagerImpl(
                Arrays.asList(demoConfig("ds1", 3306)
                        , demoConfig("ds2", 3307))));
    }

    private void forceClose(DruidPooledConnection mySQLConnection) throws SQLException {
        DruidPooledConnection mySQLConnection1 = mySQLConnection;
        mySQLConnection1.getConnection().close();
        mySQLConnection1.abandond();
        mySQLConnection1.close();
    }

    @BeforeEach
    public  void before() {
        connectionMap.put("ds1", XaTestSuite.getMySQLConnection(DB1));
        connectionMap.put("ds2", XaTestSuite.getMySQLConnection(DB2));
    }

    @AfterEach
    public  void after() throws Exception {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            String k = entry.getKey();
            Connection v = entry.getValue();
            v.close();
        }
    }

    @Test
    public void nextXid(VertxTestContext testContext) throws Exception {
        XaLog demoRepository = getDemoRepository();
        int start = Integer.parseInt(demoRepository.nextXid());
        int end = Integer.parseInt(demoRepository.nextXid());
        Assertions.assertEquals(1,
                end - start);
        testContext.completeNow();
    }

    @Test
    public void commitOnePhaseButRecoveryNoEffect(VertxTestContext testContext) throws Exception {
        XaLog demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.readXARecoveryLog(connectionMap);
            try {
                extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
                extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT_ONE_PHASE);
                testContext.completeNow();
            } catch (SQLException throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        }
    }

    @Test
    public void commitOnePhaseButRecoveryNoEffect2(VertxTestContext testContext) throws SQLException, IOException {
        XaLog demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.readXARecoveryLog(connectionMap);
            try {
                extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT_ONE_PHASE);
                testContext.completeNow();
            } catch (SQLException throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        }

    }


    @Test
    public void xaRecover(VertxTestContext testContext) throws Exception {
        Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
        try {
            extracteXaCmd("x.0", mySQLConnection, XaSqlConnection.XA_COMMIT);
        } catch (Throwable i) {
        }
        XaLog demoRepository = getDemoRepository();
        String xid = demoRepository.nextXid();
        demoRepository.beginXa(xid);

        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
        demoRepository.log(xid, "ds1", State.XA_STARTED);
        demoRepository.log(xid, "ds1", State.XA_ENDED);
        demoRepository.log(xid, "ds1", State.XA_PREPARED);
        forceClose((DruidPooledConnection) mySQLConnection);
        demoRepository.readXARecoveryLog(connectionMap);
        try {
            Connection
                    connection = XaTestSuite.getMySQLConnection(DB1);
            Assertions.assertTrue(
                    JdbcUtils
                            .executeQuery(connection, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
            JdbcUtils.close(connection);
            testContext.completeNow();
        } catch (Exception throwables) {
            testContext.failNow(throwables);
        } finally {
            JdbcUtils.close(mySQLConnection);
        }
    }


    @Test
    public void xaRecoverButCommited(VertxTestContext testContext) throws Exception {
        XaLog demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);
            demoRepository.log(xid, "ds1", State.XA_COMMITED);
            demoRepository.readXARecoveryLog(connectionMap);
            try {
                Assertions.assertTrue(
                        JdbcUtils
                                .executeQuery(mySQLConnection, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());

                testContext.completeNow();
            } catch (SQLException throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        }
    }

    @Test
    public void xaFailAndXARecoverCommit(VertxTestContext testContext) throws Exception {
        XaLog demoRepository = getDemoRepository();
        demoRepository.readXARecoveryLog(connectionMap);
        {
            String xid = "367";
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, "delete from mycat.xa_log");
            Connection mySQLConnection2 = XaTestSuite.getMySQLConnection(DB2);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection, "insert mycat.xa_log (xid) values ('%s')");
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_START);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_END);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_PREPARE);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT);
            forceClose((DruidPooledConnection) mySQLConnection2);

            demoRepository.log(xid, "ds1", State.XA_STARTED);

            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);
            demoRepository.log(xid, "ds1", State.XA_COMMITED);

            demoRepository.log(xid, "ds2", State.XA_STARTED);
            demoRepository.log(xid, "ds2", State.XA_ENDED);
            demoRepository.log(xid, "ds2", State.XA_PREPARED);

            //recover connection
            after();
            before();

            demoRepository.readXARecoveryLog(connectionMap);
            try {
                Connection mySQLConnection1 = XaTestSuite.getMySQLConnection(DB2);
                Assertions.assertTrue(
                        JdbcUtils
                                .executeQuery(mySQLConnection1, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
                JdbcUtils.close(mySQLConnection1);
                testContext.completeNow();
            } catch (SQLException throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        }
    }

    @Test
    public void xaFailAndXARecoverRollback(VertxTestContext testContext) throws Exception {
        XaLog demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            Connection mySQLConnection2 = XaTestSuite.getMySQLConnection(DB2);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_START);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_END);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_PREPARE);

            forceClose((DruidPooledConnection) mySQLConnection);
            forceClose((DruidPooledConnection) mySQLConnection2);

            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);


            demoRepository.log(xid, "ds2", State.XA_STARTED);
            demoRepository.log(xid, "ds2", State.XA_ENDED);
            demoRepository.log(xid, "ds2", State.XA_PREPARED);


            demoRepository.readXARecoveryLog(connectionMap);
            try {
                Connection mySQLConnection1 = XaTestSuite.getMySQLConnection(DB2);
                Assertions.assertTrue(
                        JdbcUtils
                                .executeQuery(mySQLConnection1, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
                JdbcUtils.close(mySQLConnection1);
                testContext.completeNow();
            } catch (SQLException throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        }
    }

    private void extracteXaCmd(String xid, Connection mySQLConnection, String cmd) throws SQLException {
        JdbcUtils.execute(mySQLConnection, String.format(cmd, xid));
    }
}