package io.mycat.statistic.histogram;

import io.mycat.MetadataManager;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.statistic.StatisticCenter;

import java.util.TreeMap;

public class MycatHistogram {
    final TreeMap<Comparable, Double> rangeRowCountmap;
    final TreeMap<Comparable, Double> distinctValueMap;

    public MycatHistogram( TreeMap<Comparable, Double> rangeRowCountMap, TreeMap<Comparable, Double> distinctValueMap) {
        this.rangeRowCountmap = rangeRowCountMap;
        this.distinctValueMap = distinctValueMap;
    }

    public static MycatHistogram of(Double rowCount ,
                                    MetadataManager metadataManager,
                                    String schemaName,
                                    String tableName,
                                    String columnName,
                                    MySQLHistogram mySQLHistogram) {
        TableHandler table = metadataManager.getTable(schemaName, tableName);
        SimpleColumnInfo column = table.getColumnByName(columnName);
        TreeMap<Comparable, Double> rangeRowCountMap = new TreeMap<>();
        TreeMap<Comparable, Double> distinctValueMap = new TreeMap<>();
        if (mySQLHistogram.isEquiHeight()) {
            for (MySQLBucket mySQLBucket : mySQLHistogram.getMySQLBuckets()) {
                Object lowerInclusiveText = mySQLBucket.getLowerInclusiveValue();
                Object upperInclusiveText = mySQLBucket.getUpperInclusiveValue();
                Comparable lowerInclusiveValue = (Comparable) column.normalizeValue(lowerInclusiveText);
                Comparable upperInclusiveValue = (Comparable) column.normalizeValue(upperInclusiveText);
                double v = mySQLBucket.getCumulativeFrequence() * rowCount;
                rangeRowCountMap.put(lowerInclusiveValue, v);
                rangeRowCountMap.put(upperInclusiveValue, v);

                distinctValueMap.put(lowerInclusiveValue, mySQLBucket.getNumberOfDistinctValues());
                distinctValueMap.put(upperInclusiveValue, mySQLBucket.getNumberOfDistinctValues());
            }
        } else {
            for (MySQLBucket mySQLBucket : mySQLHistogram.getMySQLBuckets()) {
                Object valueText = mySQLBucket.getLowerInclusiveValue();
                Object value = column.normalizeValue(valueText);
                rangeRowCountMap.put((Comparable) value, mySQLBucket.getCumulativeFrequence() * rowCount);
                distinctValueMap.put((Comparable) value, 1.0);
            }
        }
        return new MycatHistogram(rangeRowCountMap, distinctValueMap);
    }

    public static void main(String[] args) {

    }

    public MycatHistogram merge(MycatHistogram mycatHistogram2) {
        TreeMap<Comparable, Double> rangeRowCountmap = new TreeMap<>(this.rangeRowCountmap);
        rangeRowCountmap.putAll(mycatHistogram2.rangeRowCountmap);

        TreeMap<Comparable, Double> distinctValueMap = new TreeMap<>(this.distinctValueMap);
        distinctValueMap.putAll(mycatHistogram2.distinctValueMap);
        return new MycatHistogram(rangeRowCountmap,distinctValueMap);
    }
}
