//package io.mycat.api.collector;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.beans.mycat.CopyMycatRowMetaData;
//import io.mycat.beans.mycat.MycatRowMetaData;
//import io.mycat.beans.mycat.ResultSetBuilder;
//import net.sf.ehcache.Cache;
//import net.sf.ehcache.CacheManager;
//import net.sf.ehcache.Element;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class RowBaseIteratorCacher implements Runnable {
//    final RowBaseIterator iterator;
//    final Object key;
//
//    static final CacheManager cacheManager = CacheManager.create();
//    static final String cacheName = "resultSet";
//
//    public static void put(Object key, RowBaseIterator rowBaseIterator) {
//        new RowBaseIteratorCacher(key, rowBaseIterator).run();
//    }
//
//    public static RowBaseIterator get(Object key) {
//        Cache cache = cacheManager.getCache(cacheName);
//        Element element = cache.get(key);
//        if (element == null) {
//            return null;
//        }
//        return (RowBaseIterator) element.getObjectValue();
//    }
//
//    public RowBaseIteratorCacher(Object key, RowBaseIterator iterator) {
//        this.key = key;
//        this.iterator = iterator;
//    }
//
//    @Override
//    public void run() {
//        MycatRowMetaData metaData = iterator.getMetaData();
//        CopyMycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(metaData);
//        int columnCount = metaData.getColumnCount();
//        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
//        ArrayList<Object[]> rows = new ArrayList<>();
//        while (iterator.next()) {
//            Object[] row = new Object[columnCount];
//            for (int i = 0; i < columnCount; i++) {
//                row[i] = iterator.getObject(i + 1);
//                rows.add(row);
//            }
//        }
//        ImmutableList<Object[]> objects = ImmutableList.copyOf(rows);
//        new ResultSetBuilder.DefObjectRowIteratorImpl(mycatRowMetaData, )
//        RowBaseIterator rowBaseIterator = resultSetBuilder.build(mycatRowMetaData);
//        Cache cache = cacheManager.getCache(cacheName);
//        Element element = new Element(key, rowBaseIterator);
//        cache.put(element);
//    }
//}