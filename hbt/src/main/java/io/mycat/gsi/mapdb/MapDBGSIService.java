//package io.mycat.gsi.mapdb;
//
//import io.mycat.MetadataManager;
//import io.mycat.SimpleColumnInfo;
//import io.mycat.TableHandler;
//import io.mycat.gsi.GSIService;
//import org.mapdb.DB;
//import org.mapdb.DBMaker;
//
//import java.io.File;
//import java.util.*;
//
//public class MapDBGSIService implements GSIService {
//    private final MapDBRepository repository;
//
//    public MapDBGSIService(File file, MetadataManager metadataManager) {
////        DB db = DBMaker.fileDB(file).make();
//        this.repository = new MapDBRepository(null, metadataManager);
//    }
//
//    @Override
//    public Optional<Iterable<Object[]>> scanProject(String schemaName, String tableName, int[] projects) {
//        return Optional.empty();
//    }
//
//    @Override
//    public Optional<Iterable<Object[]>> scan(String schemaName, String tableName) {
//        return Optional.empty();
//    }
//
//    @Override
//    public Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName, int index, Object value) {
//        return Optional.empty();
//    }
//
//    @Override
//    public Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName, int[] projects, int[] filterIndexes, Object[] values) {
//        return Optional.empty();
//    }
//
//    @Override
//    public Collection<String> queryDataNode(String schemaName, String tableName, int index, Object value) {
//        MetadataManager metadataManager = getMetadataManager();
//        TableHandler table = metadataManager.getTable(schemaName, tableName);
//        SimpleColumnInfo columnInfo = table.getColumns().get(index);
//        Map<String, IndexStorage> indexStorageMap = repository.getIndexStorageMap(schemaName, tableName);
//
//        IndexStorage indexStorage = IndexChooser.HIT_MAX_COLUMNS.choseIndex(indexStorageMap.values(), new SimpleColumnInfo[]{columnInfo});
//        Collection<RowIndexValues> rowIndexValues = indexStorage.getByPrefix(value);
//        Set<String> dataNodeSet = new LinkedHashSet<>();
//        for (RowIndexValues rowIndexValue : rowIndexValues) {
//            dataNodeSet.addAll(rowIndexValue.getDataNodeKeyList());
//        }
//        return dataNodeSet;
//    }
//
//    @Override
//    public boolean isIndexTable(String schemaName, String tableName) {
//        Map<String, IndexStorage> indexStorageMap = repository.getIndexStorageMap(schemaName, tableName);
//        return indexStorageMap != null;
//    }
//
//    @Override
//    public void insert(String txId, String schemaName, String tableName, SimpleColumnInfo[] columns, List<Object> values, String dataNodeKey) {
//        repository.insert(txId, schemaName, tableName, columns, values, dataNodeKey);
//    }
//
//    @Override
//    public void updateByPrimaryKey(String txId, String schemaName, String tableName, Map<SimpleColumnInfo, Object> setValues, Collection<Map<SimpleColumnInfo, Object>> whereList, String dataNodeKey) {
//
//    }
//
//    @Override
//    public boolean preCommit(String txId) {
//        return repository.preCommit(txId);
//    }
//
//    @Override
//    public boolean commit(String txId) {
//        return repository.commit(txId);
//    }
//
//    @Override
//    public boolean rollback(String txId) {
//        return repository.rollback(txId);
//    }
//
//    public MetadataManager getMetadataManager() {
//        return repository.getMetadataManager();
//    }
//
//}
