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

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SavepointSqlConnection;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import cn.mycat.vertx.xa.impl.BaseXaSqlConnection;
import com.google.gson.annotations.SerializedName;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

@net.jcip.annotations.NotThreadSafe
public class BaseXaSavepointTest extends BaseSavepointSuite {
    public BaseXaSavepointTest() throws Exception {
        super(new TestMySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3306)
                , demoConfig("ds2", 3307))), new BiFunction<MySQLManager, XaLog, XaSqlConnection>() {
            @Override
            public XaSqlConnection apply(MySQLManager mySQLManager, XaLog xaLog) {
                return new SavepointSqlConnection(new BaseXaSqlConnection(() -> mySQLManager, xaLog));
            }
        });
    }


}
