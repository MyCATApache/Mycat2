//package io.mycat.gsi.mapdb;
//
//import io.mycat.IndexInfo;
//import io.mycat.SimpleColumnInfo;
//import io.mycat.gsi.GSIService.IndexValue;
//import io.mycat.gsi.GSIService.RowIndexValues;
//import io.mycat.util.LazyTransformCollection;
//import lombok.Getter;
//import lombok.Setter;
//import org.mapdb.BTreeMap;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.concurrent.ConcurrentNavigableMap;
//import java.util.function.Function;
//
///**
// * 索引存储
// */
//@Getter
//@Setter
//public class IndexStorage {
//    /**
//     * 索引定义元信息
//     */
//    private IndexInfo indexInfo;
//    /**
//     * 索引存储数据
//     * key = 索引键
//     * value = [0]数据节点信息 [1]~[N]覆盖字段
//     */
//    private BTreeMap<Object[], Object[]> storage;
//    private Function<SimpleColumnInfo,Class> typeMap;
//
//    /**
//     * 左前缀扫描
//     * @param prefixs 前缀键
//     * @return 索引信息
//     */
//    public Collection<RowIndexValues> getByPrefix(Object...prefixs){
//        SimpleColumnInfo[] indexColumnInfos = indexInfo.getIndexes();
//        for (int i = 0; i < prefixs.length; i++) {
//            prefixs[i] = cast(indexColumnInfos[i],prefixs[i]);
//        }
//        ConcurrentNavigableMap<Object[], Object[]> subMap = storage.prefixSubMap(prefixs);
//        return LazyTransformCollection.transform(subMap.entrySet(),
//                entry -> parse(entry.getKey(),entry.getValue()));
//    }
//
//    public Object cast(SimpleColumnInfo columnInfo,Object value){
//        Object cast = MapDBUtils.cast(value, typeMap.apply(columnInfo));
//        return cast;
//    }
//
//    public RowIndexValues parse(Object[] keys, Object[] values){
//        RowIndexValues rowIndexValues = new RowIndexValues(indexInfo);
//
//        // 索引键信息
//        for (int i = 0; i < keys.length; i++) {
//            SimpleColumnInfo columnInfo = indexInfo.getIndexes()[i];
//            Object value = cast(columnInfo,keys[i]);
//            rowIndexValues.getIndexes().add(new IndexValue(columnInfo,value));
//        }
//
//        // values第一位存放数据节点信息
//        String dataNodeKeys = (String) values[0];
//        if(dataNodeKeys != null && dataNodeKeys.length() > 0) {
//            rowIndexValues.getDataNodeKeyList().addAll(Arrays.asList(dataNodeKeys.split(",")));
//        }
//
//        // values后续存放覆盖字段
//        for (int i = 1; i < values.length; i++) {
//            SimpleColumnInfo columnInfo = indexInfo.getCovering()[i - 1];
//            Object value = cast(columnInfo,values[i]);
//            rowIndexValues.getCoverings().add(new IndexValue(columnInfo,value));
//        }
//        return rowIndexValues;
//    }
//
//    @Override
//    public String toString() {
//        return indexInfo.toString();
//    }
//}
