//package io.mycat.gsi.mapdb;
//
//import io.mycat.*;
//import io.mycat.calcite.table.SchemaHandler;
//import io.mycat.gsi.GSIService.IndexValue;
//import io.mycat.gsi.GSIService.RowIndexValues;
//import io.mycat.util.NameMap;
//import lombok.extern.slf4j.Slf4j;
//
//import java.math.BigDecimal;
//import java.sql.JDBCType;
//import java.util.*;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
///**
// * https://jankotek.gitbooks.io/mapdb/content/db/
// * 数据结构实现来自： https://github.com/jankotek/mapdb
// *
// * @author wangzihaogithub
// */
//@Slf4j
//public class MapDBRepository {
//
//    /**
//     * Map的层次为
//     * => 目录名(schema)
//     *    => 表名
//     *      => 索引名
//     *          => 索引数据
//     */
//    private Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap;
//    private MetadataManager metadataManager;
//    /**
//     * 物理存储
//     */
//    private final DB db;
//    /**
//     * 字段类型映射
//     */
//    private final Map<JDBCType,Class> typeClassMap = new HashMap<>();
//    private Function<SimpleColumnInfo,Class> typeMap = columnInfo -> typeClassMap.get(columnInfo.getJdbcType());
//
//    public MapDBRepository(DB db, MetadataManager metadataManager) {
//        this.db = db;
//        this.metadataManager = metadataManager;
//        // 数字
//        typeClassMap.put(JDBCType.BIT,Byte.class);
//        typeClassMap.put(JDBCType.TINYINT,Integer.class);
//        typeClassMap.put(JDBCType.SMALLINT,Integer.class);
//        typeClassMap.put(JDBCType.INTEGER,Integer.class);
//        typeClassMap.put(JDBCType.BIGINT,Long.class);
//        typeClassMap.put(JDBCType.FLOAT,BigDecimal.class);
//        typeClassMap.put(JDBCType.REAL,BigDecimal.class);
//        typeClassMap.put(JDBCType.DOUBLE,BigDecimal.class);
//        typeClassMap.put(JDBCType.NUMERIC,BigDecimal.class);
//        typeClassMap.put(JDBCType.DECIMAL,BigDecimal.class);
//
//        // 文本
//        typeClassMap.put(JDBCType.CHAR,String.class);
//        typeClassMap.put(JDBCType.VARCHAR,String.class);
//        typeClassMap.put(JDBCType.LONGVARCHAR,String.class);
//        typeClassMap.put(JDBCType.NULL,String.class);
//        typeClassMap.put(JDBCType.OTHER,String.class);
//        typeClassMap.put(JDBCType.JAVA_OBJECT,String.class);
//        typeClassMap.put(JDBCType.DISTINCT,String.class);
//        typeClassMap.put(JDBCType.STRUCT,String.class);
//        typeClassMap.put(JDBCType.ARRAY,String.class);
//        typeClassMap.put(JDBCType.REF,String.class);
//        typeClassMap.put(JDBCType.DATALINK,String.class);
//        typeClassMap.put(JDBCType.BOOLEAN,String.class);
//        typeClassMap.put(JDBCType.ROWID,String.class);
//        typeClassMap.put(JDBCType.NCHAR,String.class);
//        typeClassMap.put(JDBCType.NVARCHAR,String.class);
//        typeClassMap.put(JDBCType.LONGNVARCHAR,String.class);
//        typeClassMap.put(JDBCType.NCLOB,String.class);
//        typeClassMap.put(JDBCType.SQLXML,String.class);
//        typeClassMap.put(JDBCType.REF_CURSOR,String.class);
//
//        // 日期
//        typeClassMap.put(JDBCType.DATE,Date.class);
//        typeClassMap.put(JDBCType.TIME,Date.class);
//        typeClassMap.put(JDBCType.TIMESTAMP,Date.class);
//        typeClassMap.put(JDBCType.TIME_WITH_TIMEZONE,Date.class);
//        typeClassMap.put(JDBCType.TIMESTAMP_WITH_TIMEZONE,Date.class);
//
//        // 字节
//        typeClassMap.put(JDBCType.BINARY,byte[].class);
//        typeClassMap.put(JDBCType.VARBINARY,byte[].class);
//        typeClassMap.put(JDBCType.LONGVARBINARY,byte[].class);
//        typeClassMap.put(JDBCType.BLOB,byte[].class);
//        typeClassMap.put(JDBCType.CLOB,byte[].class);
//
//        Runtime.getRuntime().addShutdownHook(new Thread("mapDB-gsi"){
//            @Override
//            public void run() {
//                log.info("before close");
//                try {
//                    db.close();
//                }finally {
//                    log.info("after close");
//                }
//            }
//        });
//    }
//
//    public MetadataManager getMetadataManager() {
//        MetadataManager wrapper = MetaClusterCurrent.wrapper(MetadataManager.class);
//        if(wrapper != null){
//            return wrapper;
//        }
//        return metadataManager;
//    }
//
//    public int drop(String schemaName,String tableName, String indexName) {
//        return 0;
//    }
//
//    private boolean isHitIndex(IndexInfo indexInfo, int[] columnNames){
//        for (SimpleColumnInfo columnInfo : indexInfo.getIndexes()) {
//            for (int columnName : columnNames) {
//                if (columnInfo.getId() == columnName) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }
//
//    private Object getValue(SimpleColumnInfo columnInfo, SimpleColumnInfo[] columns,List<Object> values){
//        for (int i = 0; i < columns.length; i++) {
//            if(columnInfo.getId() == columns[i].getId()){
//                return values.get(i);
//            }
//        }
//        return null;
//    }
//
//    private List<RowIndexValues> getRowIndexValuesList(Map<String,IndexInfo> indexes, SimpleColumnInfo[] columns, List<Object> values){
//        List<RowIndexValues> rowIndexValuesList = new ArrayList<>();
//        for (IndexInfo indexInfo : indexes.values()) {
//            RowIndexValues rowIndexValues = new RowIndexValues(indexInfo);
//            for (SimpleColumnInfo columnInfo : indexInfo.getIndexes()) {
//                Object value = getValue(columnInfo, columns, values);
//                rowIndexValues.getIndexes().add(new IndexValue(columnInfo,value));
//            }
//            for (SimpleColumnInfo columnInfo : indexInfo.getCovering()) {
//                Object value = getValue(columnInfo, columns, values);
//                rowIndexValues.getCoverings().add(new IndexValue(columnInfo,value));
//            }
//            rowIndexValuesList.add(rowIndexValues);
//        }
//        return rowIndexValuesList;
//    }
//
//    public void insert(String txId, String schemaName, String tableName, SimpleColumnInfo[] columns, List<Object> values, String dataNodeKey) {
//        TableHandler table = getMetadataManager().getTable(schemaName, tableName);
//        Map<String,IndexInfo> indexMap = table.getIndexes();
//        if(indexMap == null){
//            return;
//        }
//
//        Map<String, Map<String, IndexStorage>> tableIndexMap = getSchemaTableIndexStorageMap().get(schemaName);
//        if(tableIndexMap == null){
//            return;
//        }
//        Map<String, IndexStorage> indexStorageMap = tableIndexMap.get(tableName);
//        if(indexStorageMap == null){
//            return;
//        }
//        List<RowIndexValues> rowIndexValuesList = getRowIndexValuesList(indexMap, columns, values);
//        for (RowIndexValues rowIndexValues : rowIndexValuesList) {
//            IndexInfo indexInfo = rowIndexValues.getIndexInfo();
//            IndexStorage indexStorage = indexStorageMap.get(indexInfo.getIndexName());
//
//            List<Object> storageKeys = rowIndexValues.getIndexes().stream().map(this::getValueAndCast).collect(Collectors.toList());
//            List<Object> storageValues = rowIndexValues.getCoverings().stream().map(this::getValueAndCast).collect(Collectors.toList());
//            storageValues.add(0,dataNodeKey);
//
//            indexStorage.getStorage().put(storageKeys.toArray(),storageValues.toArray());
//        }
//        db.commit();
//    }
//
//    private Object getValueAndCast(IndexValue indexValue){
//        Object value = indexValue.getValue();
//        Class javaClass = typeMap.apply(indexValue.getColumn());
//        Object result = MapDBUtils.cast(value, javaClass);
//        return result;
//    }
//
//    public Map<String, IndexStorage> getIndexStorageMap(String schemaName, String tableName){
//        Map<String, Map<String, Map<String, IndexStorage>>> schemaTableIndexStorageMap = getSchemaTableIndexStorageMap();
//        Map<String, Map<String, IndexStorage>> tableIndexStorageMap = schemaTableIndexStorageMap.get(schemaName);
//        if(tableIndexStorageMap == null){
//            return null;
//        }
//        return tableIndexStorageMap.get(tableName);
//    }
//
//    public boolean preCommit(String txId) {
//        return true;
//    }
//
//    public boolean commit(String txId) {
//        //todo 事物id未实行
//        try {
//            db.commit();
//            return true;
//        }catch (Error error){
//            return false;
//        }catch (Exception e){
//            return false;
//        }
//    }
//
//    public boolean rollback(String txId) {
//        try {
//            db.rollback();
//            return true;
//        }catch (Error error){
//            return false;
//        }catch (Exception e){
//            return false;
//        }
//    }
//
//    /**
//     * 重新定义索引
//     * @param metadataManager 元数据
//     * @return 索引定义
//     */
//    public Map<String,Map<String,Map<String, IndexStorage>>> reDefinitionIndex(MetadataManager metadataManager){
//        Map<String,Map<String,Map<String, IndexStorage>>> old = this.schemaTableIndexStorageMap;
//        this.schemaTableIndexStorageMap = definitionIndex(metadataManager);
//        return old;
//    }
//
//    public Map<String, Map<String, Map<String, IndexStorage>>> getSchemaTableIndexStorageMap() {
//        if(schemaTableIndexStorageMap == null || schemaTableIndexStorageMap.isEmpty()){
//            this.schemaTableIndexStorageMap = definitionIndex(getMetadataManager());
//        }
//        return schemaTableIndexStorageMap;
//    }
//
//    public Map<String,Map<String,Map<String, IndexStorage>>> definitionIndex(MetadataManager metadataManager){
//        Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap = new LinkedHashMap<>();
//        NameMap<SchemaHandler> schemaMap = metadataManager.getSchemaMap();
//        for (SchemaHandler schemaHandler : schemaMap.values()) {
//            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
//                Map<String, IndexInfo> indexes = tableHandler.getIndexes();
//                if(indexes == null){
//                    continue;
//                }
//                for (IndexInfo indexInfo : indexes.values()) {
//                    IndexStorage indexStorage = buildIndexData(indexInfo);
//                    putIndex(indexStorage,schemaTableIndexStorageMap);
//                }
//            }
//        }
//        return schemaTableIndexStorageMap;
//    }
//
//    private IndexStorage putIndex(IndexStorage indexStorage,Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap){
//        Map<String, Map<String, IndexStorage>> tableIndexMap = schemaTableIndexStorageMap.computeIfAbsent(
//                indexStorage.getIndexInfo().getSchemaName(), k -> new LinkedHashMap<>());
//
//        Map<String, IndexStorage> indexMap = tableIndexMap.computeIfAbsent(
//                indexStorage.getIndexInfo().getTableName(), k -> new LinkedHashMap<>());
//
//        return indexMap.put(indexStorage.getIndexInfo().getIndexName(),indexStorage);
//    }
//
//    private IndexStorage buildIndexData(IndexInfo indexInfo){
//        IndexStorage index = new IndexStorage();
//        index.setIndexInfo(indexInfo);
//
//        Serializer[] keySerializers = new Serializer[indexInfo.getIndexes().length];
//        Serializer[] valueSerializers = new Serializer[indexInfo.getCovering().length + 1];
//        Class[] keyTypes = new Class[keySerializers.length];
//        Class[] valueTypes = new Class[valueSerializers.length];
//
//        for (int i = 0; i < keySerializers.length; i++) {
//            SimpleColumnInfo columnInfo = indexInfo.getIndexes()[i];
//            Class type = typeMap.apply(columnInfo);
//            if(type == null){
//                throw new IllegalStateException("不支持的字段类型" + columnInfo);
//            }
//            keyTypes[i] = type;
//            keySerializers[i] = SerializerUtils.serializerForClass(type);
//        }
//
//        valueSerializers[0] = SerializerUtils.serializerForClass(String.class);
//        for (int i = 1; i < valueSerializers.length; i++) {
//            SimpleColumnInfo columnInfo = indexInfo.getCovering()[i-1];
//            Class type = typeMap.apply(columnInfo);
//            if(type == null){
//                throw new IllegalStateException("不支持的字段类型" + columnInfo);
//            }
//            valueTypes[i] = type;
//            valueSerializers[i] = SerializerUtils.serializerForClass(type);
//        }
//
//        BTreeMap<Object[], Object[]> bTreeMap = db.treeMap(indexInfo.getSchemaName()+"."+indexInfo.getTableName()+"."+indexInfo.getIndexName())
//                .keySerializer(new SerializerArrayTuple(keySerializers))
//                .valueSerializer(new SerializerArrayTuple(valueSerializers))
//                .createOrOpen();
//        index.setStorage(bTreeMap);
//        index.setTypeMap(typeMap);
//        return index;
//    }
//
//}
