package io.mycat.gsi.mapdb;

import io.mycat.IndexInfo;
import io.mycat.MetaClusterCurrent;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.gsi.GSIService.IndexValue;
import io.mycat.gsi.GSIService.RowIndexValues;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.util.NameMap;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.mapdb.serializer.SerializerUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * https://jankotek.gitbooks.io/mapdb/content/db/
 * 数据结构实现来自： https://github.com/jankotek/mapdb
 *
 * @author wangzihaogithub
 */
@Slf4j
public class MapDBRepository {

    /**
     * Map的层次为
     * => 目录名(schema)
     *    => 表名
     *      => 索引名
     *          => 索引数据
     */
    private Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap = new LinkedHashMap<>();
    private MetadataManager metadataManager;
    /**
     * 物理存储
     */
    private final DB db;
    /**
     * 字段类型映射
     */
    private final Map<SimpleColumnInfo.Type,Class> typeClassMap = new HashMap<>();

    public MapDBRepository(DB db) {
        this(db,null);
    }

    public MapDBRepository(DB db, MetadataManager metadataManager) {
        this.db = db;
        this.metadataManager = metadataManager;
        typeClassMap.put(SimpleColumnInfo.Type.STRING,String.class);
        typeClassMap.put(SimpleColumnInfo.Type.NUMBER,BigDecimal.class);
        typeClassMap.put(SimpleColumnInfo.Type.BLOB,byte[].class);
        typeClassMap.put(SimpleColumnInfo.Type.TIMESTAMP,Date.class);
        typeClassMap.put(SimpleColumnInfo.Type.DATE,Date.class);
        typeClassMap.put(SimpleColumnInfo.Type.TIME,Date.class);

        Runtime.getRuntime().addShutdownHook(new Thread("mapDB-gsi"){
            @Override
            public void run() {
                log.info("before close");
                try {
                    db.close();
                }finally {
                    log.info("after close");
                }
            }
        });
    }

    public MetadataManager getMetadataManager() {
        MetadataManager wrapper = MetaClusterCurrent.wrapper(MetadataManager.class);
        if(wrapper != null){
            return wrapper;
        }
        return metadataManager;
    }

    public int drop(String schemaName,String tableName, String indexName) {
        return 0;
    }

    private boolean isHitIndex(IndexInfo indexInfo, int[] columnNames){
        for (SimpleColumnInfo columnInfo : indexInfo.getIndexes()) {
            for (int columnName : columnNames) {
                if (columnInfo.getId() == columnName) {
                    return true;
                }
            }
        }
        return false;
    }

    private Object getValue(SimpleColumnInfo columnInfo, int[] columnNames,List<Object> values){
        for (int i = 0; i < columnNames.length; i++) {
            if(columnInfo.getId() == columnNames[i]){
                return values.get(i);
            }
        }
        return null;
    }

    private List<RowIndexValues> getRowIndexValuesList(Map<String,IndexInfo> indexes, int[] columnNames, List<Object> values){
        List<RowIndexValues> rowIndexValuesList = new ArrayList<>();
        for (IndexInfo indexInfo : indexes.values()) {
            RowIndexValues rowIndexValues = new RowIndexValues(indexInfo);
            for (SimpleColumnInfo columnInfo : indexInfo.getIndexes()) {
                Object value = getValue(columnInfo, columnNames, values);
                rowIndexValues.getIndexes().add(new IndexValue(columnInfo,value));
            }
            for (SimpleColumnInfo columnInfo : indexInfo.getCovering()) {
                Object value = getValue(columnInfo, columnNames, values);
                rowIndexValues.getCoverings().add(new IndexValue(columnInfo,value));
            }
            rowIndexValuesList.add(rowIndexValues);
        }
        return rowIndexValuesList;
    }

    public void insert(String txId, String schemaName, String tableName, int[] columnNames, List<Object> values, List<String> dataNodeKeyList) {
        TableHandler table = getMetadataManager().getTable(schemaName, tableName);
        Map<String,IndexInfo> indexeMap = table.getIndexes();
        if(indexeMap == null){
            return;
        }

        Map<String, Map<String, IndexStorage>> tableIndexMap = getSchemaTableIndexStorageMap().get(schemaName);
        if(tableIndexMap == null){
            return;
        }
        Map<String, IndexStorage> indexStorageMap = tableIndexMap.get(tableName);
        if(indexStorageMap == null){
            return;
        }
        List<RowIndexValues> rowIndexValuesList = getRowIndexValuesList(indexeMap, columnNames, values);
        for (RowIndexValues rowIndexValues : rowIndexValuesList) {
            IndexInfo indexInfo = rowIndexValues.getIndexInfo();
            IndexStorage indexStorage = indexStorageMap.get(indexInfo.getIndexName());

            List<Object> storageKeys = rowIndexValues.getIndexes().stream().map(IndexValue::getValue).collect(Collectors.toList());
            List<Object> storageValues = rowIndexValues.getCoverings().stream().map(IndexValue::getValue).collect(Collectors.toList());
            storageValues.add(0,String.join(",", rowIndexValues.getDataNodeKeyList()));

            indexStorage.getStorage().put(storageKeys.toArray(),storageValues.toArray());
        }
        db.commit();
    }

    public Map<String, IndexStorage> getIndexStorageMap(String schemaName, String tableName){
        Map<String, Map<String, Map<String, IndexStorage>>> schemaTableIndexStorageMap = getSchemaTableIndexStorageMap();
        Map<String, Map<String, IndexStorage>> tableIndexStorageMap = schemaTableIndexStorageMap.get(schemaName);
        if(tableIndexStorageMap == null){
            return null;
        }
        return tableIndexStorageMap.get(tableName);
    }

    public boolean preCommit(Long txId) {
        return true;
    }

    public boolean commit(Long txId) {
        //todo 事物id未实行
        try {
            db.commit();
            return true;
        }catch (Error error){
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean rollback(Long txId) {
        try {
            db.rollback();
            return true;
        }catch (Error error){
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 重新定义索引
     * @param metadataManager 元数据
     * @return 索引定义
     */
    public Map<String,Map<String,Map<String, IndexStorage>>> reDefinitionIndex(MetadataManager metadataManager){
        Map<String,Map<String,Map<String, IndexStorage>>> old = this.schemaTableIndexStorageMap;
        this.schemaTableIndexStorageMap = definitionIndex(metadataManager);
        return old;
    }

    public Map<String, Map<String, Map<String, IndexStorage>>> getSchemaTableIndexStorageMap() {
        if(schemaTableIndexStorageMap.isEmpty()){
            this.schemaTableIndexStorageMap = definitionIndex(metadataManager);
        }
        return schemaTableIndexStorageMap;
    }

    public Map<String,Map<String,Map<String, IndexStorage>>> definitionIndex(MetadataManager metadataManager){
        Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap = new LinkedHashMap<>();
        NameMap<SchemaHandler> schemaMap = metadataManager.getSchemaMap();
        for (SchemaHandler schemaHandler : schemaMap.values()) {
            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
                Map<String, IndexInfo> indexes = tableHandler.getIndexes();
                if(indexes == null){
                    continue;
                }
                for (IndexInfo indexInfo : indexes.values()) {
                    IndexStorage indexStorage = buildIndexData(indexInfo);
                    putIndex(indexStorage,schemaTableIndexStorageMap);
                }
            }
        }
        return schemaTableIndexStorageMap;
    }

    private IndexStorage putIndex(IndexStorage indexStorage,Map<String,Map<String,Map<String, IndexStorage>>> schemaTableIndexStorageMap){
        Map<String, Map<String, IndexStorage>> tableIndexMap = schemaTableIndexStorageMap.computeIfAbsent(
                indexStorage.getIndexInfo().getSchemaName(), k -> new LinkedHashMap<>());

        Map<String, IndexStorage> indexMap = tableIndexMap.computeIfAbsent(
                indexStorage.getIndexInfo().getTableName(), k -> new LinkedHashMap<>());

        return indexMap.put(indexStorage.getIndexInfo().getIndexName(),indexStorage);
    }

    private IndexStorage buildIndexData(IndexInfo indexInfo){
        IndexStorage index = new IndexStorage();
        index.setIndexInfo(indexInfo);

        Serializer[] keySerializers = new Serializer[indexInfo.getIndexes().length];
        Serializer[] valueSerializers = new Serializer[indexInfo.getCovering().length + 1];

        for (int i = 0; i < keySerializers.length; i++) {
            SimpleColumnInfo columnInfo = indexInfo.getIndexes()[i];
            Class type = typeClassMap.get(columnInfo.getType());
            if(type == null){
                throw new IllegalStateException("不支持的字段类型" + columnInfo);
            }
            keySerializers[i] = SerializerUtils.serializerForClass(type);
        }

        valueSerializers[0] = SerializerUtils.serializerForClass(String.class);
        for (int i = 1; i < valueSerializers.length; i++) {
            SimpleColumnInfo columnInfo = indexInfo.getCovering()[i-1];
            Class type = typeClassMap.get(columnInfo.getType());
            if(type == null){
                throw new IllegalStateException("不支持的字段类型" + columnInfo);
            }
            valueSerializers[i] = SerializerUtils.serializerForClass(type);
        }

        BTreeMap<Object[], Object[]> bTreeMap = db.treeMap(indexInfo.getSchemaName()+"."+indexInfo.getTableName()+"."+indexInfo.getIndexName())
                .keySerializer(new SerializerArrayTuple(keySerializers))
                .valueSerializer(new SerializerArrayTuple(valueSerializers))
                .createOrOpen();
        index.setStorage(bTreeMap);
        return index;
    }

}
