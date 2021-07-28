/**
 * Copyright (C) <2021>  <mycat>
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
package io.mycat.router.mycat1xfunction;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.MycatException;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

public class PartitionByFileMap extends Mycat1xSingleValueRuleFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByFileMap.class);
    /**
     * 默认节点在map中的key
     */
    private static final String DEFAULT_NODE = "DEFAULT_NODE";
    private Map<Object, Integer> app2Partition = new HashMap<>();
    /**
     * Map<Object, Integer> app2Partition中key值的类型：默认值为0，0表示Integer，非零表示String
     */
    private Function<String, Object> transformation;
    private int partitionNum;
    /**
     * 默认节点:小于0表示不设置默认节点，大于等于0表示设置默认节点
     * <p>
     * 默认节点的作用：枚举分片时，如果碰到不识别的枚举值，就让它路由到默认节点 如果不配置默认节点（defaultNode值小于0表示不配置默认节点），碰到 不识别的枚举值就会报错， like
     * this：can't find datanode for matadata column:column_name val:ffffffff
     */
    private int defaultNode = -1;
    private String type;

    @Override
    public String name() {
        return "PartitionByFileMap";
    }

    @Override
    public void init(ShardingTableHandler tableHandler, Map<String, Object> prot, Map<String, Object> range) {
        URL resource = this.getClass().getClassLoader()
                .getResource("");
        LOGGER.info("PartitionByFileMap mapFile default path:{}", resource);
        System.out.println("PartitionByFileMap mapFile default path:" + resource);
        this.type = Objects.toString(prot.get("type"));
        defaultNode = Integer.parseInt(Objects.toString(prot.get("defaultNode")));
        switch (type) {
            case "Integer":
                transformation = Integer::parseInt;
                break;
            case "Byte":
                transformation = Byte::parseByte;
                break;
            case "Char":
                transformation = (i) -> i.charAt(0);
                break;
            case "String":
                transformation = (i) -> i;
                break;
            case "Long":
                transformation = Long::parseLong;
                break;
            case "Double":
                transformation = Double::parseDouble;
                break;
            case "Float":
                transformation = Float::parseFloat;
                break;
            case "Short":
                transformation = Short::parseShort;
                break;
            case "Boolean":
                transformation = Boolean::parseBoolean;
                break;
            case "BigInteger":
                transformation = BigInteger::new;
            case "BigDecimal":
                transformation = BigDecimal::new;
                break;
            default:
                throw new MycatException("unsupport type!!");
        }
        if (prot.get("mapFile") != null && (range == null || range.isEmpty())) {
            String mapFile = (String) prot.get("mapFile");
            InputStream fin = this.getClass().getClassLoader()
                    .getResourceAsStream(mapFile);
            if (fin == null) {
                try {
                    fin = new FileInputStream(Paths.get(mapFile).toAbsolutePath().toFile());
                } catch (IOException e) {
                    LOGGER.error("can not find file", e);
                }
            }
            if (fin == null) {
                throw new RuntimeException("can't find class resource file "
                        + mapFile);
            }
            try (BufferedReader in = new BufferedReader(new InputStreamReader(fin))) {
                for (String line = null; (line = in.readLine()) != null; ) {
                    line = line.trim();
                    if (line.startsWith("#") || line.startsWith("//")) {
                        continue;
                    }
                    int ind = line.indexOf('=');
                    if (ind < 0) {
                        continue;
                    }
                    try {
                        String key = line.substring(0, ind).trim();
                        int pid = Integer.parseInt(line.substring(ind + 1).trim());
                        app2Partition.put(transformation.apply(key), pid);
                    } catch (Exception e) {
                        LOGGER.error("PartitionByFileMap " + line + " is wrong");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("can't find class resource file "
                        + mapFile);
            } finally {
                JdbcUtils.close(fin);
            }
        } else {
            for (Entry<String, Object> entry : range.entrySet()) {
                Object key = transformation.apply(entry.getKey());
                int value = Integer.parseInt(Objects.toString(entry.getValue()));
                app2Partition.put(key, value);
            }
        }
        if (defaultNode > 0) {
            app2Partition.put(DEFAULT_NODE, defaultNode);
        }
        partitionNum = new HashSet<>(app2Partition.values()).size();
    }

    @Override
    public int calculateIndex(String columnValue) {
        Object key = transformation.apply(columnValue);
        Integer integer = null;
        try {
            integer = app2Partition.get(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        if (integer != null) {
            return integer;
        } else {
            return defaultNode;
        }
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return calculateSequenceRange(this, beginValue, endValue);
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByFileMap.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByFileMap partitionByFileMap = (PartitionByFileMap) customRuleFunction;
            Map<Object, Integer> app2Partition = partitionByFileMap.app2Partition;
            String type = partitionByFileMap.type;
            int partitionNum = partitionByFileMap.partitionNum;
            int defaultNode = partitionByFileMap.defaultNode;
            return Objects.equals(this.app2Partition, app2Partition) &&
                    Objects.equals(this.type, type) &&
                    Objects.equals(this.partitionNum, partitionNum) &&
                    Objects.equals(this.defaultNode, defaultNode);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return "" + app2Partition + type + partitionNum + defaultNode;
    }

    public static void main(String[] args) {
        PartitionByFileMap partitionByFileMap = new PartitionByFileMap();
        partitionByFileMap.init(null, null, null);
    }
}