/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
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
package io.mycat.hbt3;

import io.mycat.hbt4.DatasourceFactoryImpl;
import io.mycat.hbt4.PlanCache;
import io.mycat.util.JsonUtil;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static com.google.common.io.Files.asCharSource;

public class Main {
    public static void main(String[] args) throws Exception {
        String defaultSchema = "db1";
        String sql = "SELECT t1.id FROM travelrecord t1  join company c2 on  t1.id = c2.id ";
        URL resource = Main.class.getResource("/drds.json");
        String text = asCharSource(new File(resource.toURI()), StandardCharsets.UTF_8).read();
        DrdsConfig config = JsonUtil.from(text, DrdsConfig.class);
        DrdsRunner drdsRunners = new DrdsRunner();
        ResultSetHanlderImpl resultSetHanlder = new ResultSetHanlderImpl();
        try (DatasourceFactoryImpl datasourceFactory = new DatasourceFactoryImpl()) {
            drdsRunners.doAction(config, PlanCache.INSTANCE, datasourceFactory, defaultSchema, sql, true);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            resultSetHanlder.onError(throwable);
        }

    }

}