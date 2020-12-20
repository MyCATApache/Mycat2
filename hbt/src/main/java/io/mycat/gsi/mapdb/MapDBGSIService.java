package io.mycat.gsi.mapdb;

import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.gsi.GSIService;
import io.mycat.metadata.MetadataManager;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MapDBGSIService implements GSIService {
    private final MapDBRepository repository;

    public MapDBGSIService(String file, MetadataManager metadataManager) {
        DB db = DBMaker.fileDB(file).make();
        this.repository = new MapDBRepository(db, metadataManager);
    }

    @Override
    public Optional<Iterable<Object[]>> scanProject(String schemaName, String tableName, int[] projects) {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scan(String schemaName, String tableName) {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName, int index, Object value) {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName, int[] projects, int[] filterIndexes, Object[] values) {
        return Optional.empty();
    }

    @Override
    public Optional<DataNode> queryDataNode(String schemaName, String tableName, int index, Object value) {
        MetadataManager metadataManager = getMetadataManager();
        TableHandler table = metadataManager.getTable(schemaName, tableName);
        SimpleColumnInfo columnInfo = table.getColumns().get(index);
        Map<String, IndexStorage> indexStorageMap = repository.getIndexStorageMap(schemaName, tableName);

        IndexStorage indexStorage = IndexChooser.HIT_MAX_COLUMNS.choseIndex(indexStorageMap.values(), new SimpleColumnInfo[]{columnInfo});
        Collection<RowIndexValues> rowIndexValues = indexStorage.getByPrefix(value);
        for (RowIndexValues rowIndexValue : rowIndexValues) {
            List<String> dataNodeKeyList = rowIndexValue.getDataNodeKeyList();
            if(dataNodeKeyList.size() > 0){
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean isIndexTable(String schemaName, String tableName) {
        Map<String, IndexStorage> indexStorageMap = repository.getIndexStorageMap(schemaName, tableName);
        return indexStorageMap != null;
    }

    @Override
    public void insert(String txId, String schemaName, String tableName, int[] columnNames, List<Object> objects,List<String> dataNodeKeyList) {
        repository.insert(txId, schemaName, tableName, columnNames, objects, dataNodeKeyList);
    }

    public MetadataManager getMetadataManager() {
        return repository.getMetadataManager();
    }

}
