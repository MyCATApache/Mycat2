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
package io.mycat;

import io.mycat.util.YamlUtil;
import lombok.extern.log4j.Log4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j
public class FileConfigProvider implements ConfigProvider {
    volatile MycatConfig config;
    private String defaultPath;
    final AtomicInteger count = new AtomicInteger();

    @Override
    public void init(Map<String, String> config) throws Exception {
        this.defaultPath = config.get("path");
        fetchConfig(this.defaultPath);
    }

    @Override
    public void fetchConfig() throws Exception {
        fetchConfig(defaultPath);
    }

    @Override
    public synchronized void report(MycatConfig changed) {
        backup();
        YamlUtil.dumpToFile(defaultPath,YamlUtil.dump(changed));
        config = changed;
    }

    private void backup() {
        try {
            YamlUtil.dumpBackupToFile(defaultPath,count.getAndIncrement(),YamlUtil.dump(config));
        } catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public void fetchConfig(String url) throws Exception {
        Path asbPath = Paths.get(url).toAbsolutePath();
        if (!Files.exists(asbPath)) {
            throw new IllegalArgumentException(MessageFormat.format("path not found: {0}", Objects.toString(asbPath)));
        }
        Iterator<String> iterator = Files.lines(asbPath).iterator();
        StringBuilder sqlGroups  = new StringBuilder();
        StringBuilder full  = new StringBuilder();
        boolean in= false;
        while (iterator.hasNext()){
            String next = iterator.next();
            if (next.startsWith("#sqlGroups start")){
                sqlGroups.append(next).append('\n');
                in = true;
            }else if (in){
                sqlGroups.append(next).append('\n');
            }else if (next.startsWith("#sqlGroups end")){
                sqlGroups.append(next).append('\n');
                in =false;
            }else {
                full.append(next).append('\n');
            }
        }
        sqlGroups.append(full);
        System.out.println(sqlGroups);
        config = YamlUtil.loadText(sqlGroups.toString(), MycatConfig.class);
    }


    @Override
    public MycatConfig currentConfig() {
        return config;
    }
}