package io.mycat.router.gsi.impl;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import io.mycat.router.gsi.GSIService;
import io.mycat.util.ClassUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.mapdb.serializer.SerializerUtils;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.BiFunction;

/**
 * https://jankotek.gitbooks.io/mapdb/content/db/
 * 数据结构实现来自： https://github.com/jankotek/mapdb
 *
 * @author wangzihaogithub
 */
@Slf4j
public class MapDBGSIService implements GSIService {
    @Getter
    private String name;
    /**
     * 全局表的数据类型
     * （tableName,columnName）-> Java数据类型
     */
    private BiFunction<String,String,Class> metaDataService;
    /**
     * 物理存储
     */
    private DB db;
    /**
     * 索引定义 (ASCII)
     * 表名+索引名 => 索引列名(逗号) + 主键列名(逗号)
     */
    private BTreeMap<Object[], Object[]> indexDefineMap;
    /**
     * Map的层次为
     * 表名
     *  => 索引名
     *     => 索引数据
     */
    private final Map<String,Map<String, IndexData>> tableIndexDataMap = new LinkedHashMap<>();

    public MapDBGSIService(String gsiDbName, BiFunction<String,String,Class> metaDataService) {
        this.metaDataService = Objects.requireNonNull(metaDataService);
        this.name = Objects.requireNonNull(gsiDbName);
        String file = System.getProperty("user.dir") + "/"+gsiDbName+".db";
        this.db = DBMaker.fileDB(file).make();
        this.indexDefineMap = db.treeMap(gsiDbName)
                .keySerializer(new SerializerArrayTuple(
                        Serializer.STRING_ASCII, Serializer.STRING_ASCII))
                .valueSerializer(new SerializerArrayTuple(
                        Serializer.STRING_ASCII, Serializer.STRING_ASCII))
                .createOrOpen();
        initIndex(indexDefineMap);
        Runtime.getRuntime().addShutdownHook(new Thread("mapDB-"+getName()){
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

    public static void main(String[] args) {
        BiFunction<String,String,Class> metaDataService = (tableName,columnName)->{
            if("id".equals(columnName)){
                return Integer.class;
            }
            return String.class;
        };
        MapDBGSIService gsiService = new MapDBGSIService("gsi", metaDataService);

        //-----create------
        gsiService.create("t_order", "idx_order_no",
                new String[]{"id"},
                new String[]{"order_no"});


        //-----insert------
        IndexRowData rowData = new IndexRowData();
        rowData.setDatasourceKey("ds_1");
        rowData.setIndexColumnValues(new HashMap<>(1));
        rowData.getIndexColumnValues().put("order_no","code_1");
        rowData.setPkColumnValues(new HashMap<>(1));
        rowData.getPkColumnValues().put("id","1");

        gsiService.insert("t_order",rowData);


        //-----select------
        SQLStatement sqlStatement = SQLUtils.parseSingleStatement(
                "select id from t_order where order_no = 'code_1'",DbType.mysql, true);
        List<IndexRowData> rowDataList = gsiService.select(sqlStatement);


        //-----assert-------
        Objects.requireNonNull(rowDataList.get(0));
    }

    @Override
    public int drop(String tableName, String indexName) {
        return 0;
    }

    @Override
    public int create(String tableName, String indexName, String[] pkColumnNames, String[] indexColumnNames) {
        Object[] old = indexDefineMap.get(new String[]{tableName, indexName});
        if(old != null){
            return -1;
        }

        old = indexDefineMap.put(
                new String[]{tableName, indexName},
                new String[]{
                        String.join(",", indexColumnNames),
                        String.join(",", pkColumnNames)});

        IndexData data = buildIndexData(tableName, indexName, indexColumnNames,pkColumnNames);
        putIndex(tableName,data);
        return 1;
    }

    @Override
    public Transaction insert(String tableName, IndexRowData rowData) {
        Map<String, IndexData> indexMap = tableIndexDataMap.get(tableName);
        if(indexMap == null){
            return null;
        }
        for (IndexData index : indexMap.values()) {
            boolean existIndexData = false;

            Object[] indexValues = new Object[index.getIndexColumns().length];
            for (int i = 0; i < index.getIndexColumns().length; i++) {
                Column column = index.getIndexColumns()[i];
                Object value = rowData.getIndexColumnValues().get(column.getName());
                indexValues[i] = column.cast(value);
                existIndexData |= indexValues[i] != null;
            }
            if(!existIndexData){
                continue;
            }

            Object[] pkValues = new Object[index.getPkColumns().length];
            for (int i = 0; i < index.getPkColumns().length; i++) {
                Column column = index.getPkColumns()[i];
                Object value = rowData.getPkColumnValues().get(column.getName());
                pkValues[i] = column.cast(value);
            }
            index.getData().put(indexValues,pkValues);
        }
        return null;
    }

    @Override
    public Transaction updateByIndex(String tableName, IndexRowData rowData, Map<String, Object> index) {
        return null;
    }

    @Override
    public Transaction updateByPk(String tableName, IndexRowData rowData, Map<String, Object> pk) {
        return null;
    }

    @Override
    public Transaction deleteByIndex(String tableName, Map<String, Object> index) {
        return null;
    }

    @Override
    public Transaction deleteByPk(String tableName, Map<String, Object> pk) {
        return null;
    }

    @Override
    public boolean preCommit(Long txId) {
        return true;
    }

    @Override
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

    @Override
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

    private String getTableName(SQLStatement statement){
        //todo 这里需要测试, 以及边缘的查询情况.
        if(statement instanceof SQLSelectStatement) {
            SQLSelect select = ((SQLSelectStatement) statement).getSelect();
            SQLSelectQuery query = select.getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                SQLTableSource from = ((SQLSelectQueryBlock) query).getFrom();
                if (from instanceof SQLExprTableSource) {
                    return ((SQLExprTableSource) from).getTableName();
                }
            }
        }
        return null;
    }

    @Override
    public List<IndexRowData> select(SQLStatement statement) {
        //todo 待实现 查询引擎. 等值查询, 或查询, 范围查询, 排序, IN查询...
        String tableName = getTableName(statement);
        List<IndexRowData> rowDataList = null;
        if(statement instanceof SQLSelectStatement){
            SQLSelect select = ((SQLSelectStatement) statement).getSelect();
            SQLSelectQuery query = select.getQuery();
            if(query instanceof SQLSelectQueryBlock){
                SQLExpr where = ((SQLSelectQueryBlock) query).getWhere();
                if(where instanceof SQLBinaryOpExpr){
                    rowDataList = handleBinaryOpExpr(tableName, (SQLBinaryOpExpr) where);
                }
            }
        }
        return rowDataList;
    }

    @Override
    public Optional<Iterable<Object[]>> scanProject(int[] projects) {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scan() {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scanProjectFilter(int index, Object value) {
        return Optional.empty();
    }

    @Override
    public Optional<Iterable<Object[]>> scanProjectFilter(int[] projects, int[] filterIndexes, Object[] values) {
        return Optional.empty();
    }

    private List<IndexRowData> handleBinaryOpExpr(String tableName,SQLBinaryOpExpr where){
        Map<String, IndexData> indexDataMap = tableIndexDataMap.get(tableName);
        List<IndexRowData> result = null;
        switch (where.getOperator()){
            case Equality:{
                SQLExpr left = where.getLeft();
                SQLExpr right = where.getRight();

                if(left instanceof SQLName && right instanceof SQLValuableExpr){
                    String name = ((SQLName) left).getSimpleName();
                    Object value = ((SQLValuableExpr) right).getValue();
                    IndexData indexData = choseIndex(indexDataMap.values(), new String[]{name});
                    if(indexData == null){
                        break;
                    }
                    result = indexData.getByPrefix(value);
                }
                break;
            }
            default:{
                break;
            }
        }
        return result;
    }

    /**
     * 选择索引
     * @param indexDataList 索引列
     * @param columnNames 查询列
     * @return
     */
    private IndexData choseIndex(Collection<IndexData> indexDataList,String[] columnNames){
        int maxMatch = 0;
        IndexData result = null;
        for (IndexData indexData : indexDataList) {
            int match = indexData.match(columnNames);
            if(result == null || match > maxMatch){
                result = indexData;
            }
        }
        return result;
    }

    private int initIndex(BTreeMap<Object[], Object[]> indexDefineMap){
        indexDefineMap.forEach((keys,values) ->{
            String tableName = (String) keys[0];
            String indexName = (String) keys[1];
            String indexColumnName = (String) values[0];
            String pkColumnName = (String) values[1];

            IndexData index = buildIndexData(tableName, indexName, indexColumnName.split(","), pkColumnName.split(","));
            putIndex(tableName,index);
        });
        return 1;
    }

    private IndexData putIndex(String tableName, IndexData index){
        Map<String, IndexData> indexMap = tableIndexDataMap.computeIfAbsent(tableName, k -> new LinkedHashMap<>());
        return indexMap.put(index.getIndexName(),index);
    }

    private IndexData buildIndexData(String tableName, String indexName, String[] indexColumnNames, String[] pkColumnNames){
        Serializer[] indexSerializers = new Serializer[indexColumnNames.length];
        Serializer[] pkSerializers = new Serializer[pkColumnNames.length];
        Column[] indexColumns = new Column[indexColumnNames.length];
        Column[] pkColumns = new Column[pkColumnNames.length];

        for (int i = 0; i < indexColumnNames.length; i++) {
            Class type = metaDataService.apply(tableName, indexColumnNames[i]);
            indexSerializers[i] = SerializerUtils.serializerForClass(type);
            indexColumns[i] = new Column(indexColumnNames[i],indexSerializers[i],type);
        }
        for (int i = 0; i < pkColumnNames.length; i++) {
            Class type = metaDataService.apply(tableName, pkColumnNames[i]);
            pkSerializers[i] = SerializerUtils.serializerForClass(type);
            pkColumns[i] = new Column(pkColumnNames[i],pkSerializers[i],type);
        }

        BTreeMap<Object[], Object[]> bTreeMap = db.treeMap(tableName+"."+indexName)
                .keySerializer(new SerializerArrayTuple(indexSerializers))
                .valueSerializer(new SerializerArrayTuple(pkSerializers))
                .createOrOpen();

        IndexData index = new IndexData();
        index.setData(bTreeMap);
        index.setTableName(tableName);
        index.setIndexName(indexName);
        index.setIndexColumns(indexColumns);
        index.setPkColumns(pkColumns);
        return index;
    }

    private Serializer[] columnToSerializer(String tableName,String[] columnNames){
        Serializer[] serializers = new Serializer[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            serializers[i] = getSerializer(tableName, columnNames[i]);
        }
        return serializers;
    }

    private Serializer getSerializer(String tableName,String columnName){
        Class type = metaDataService.apply(tableName, columnName);
        Objects.requireNonNull(type);

        Serializer serializer = SerializerUtils.serializerForClass(type);
        Objects.requireNonNull(serializer);
        return serializer;
    }

    @Getter@Setter
    static class IndexData {
        private String indexName;
        private String tableName;
        private Column[] indexColumns;
        private Column[] pkColumns;
        /**
         * 数据存储
         * 索引数据 => 主键数据
         */
        private BTreeMap<Object[], Object[]> data;

        /**
         * 匹配索引
         * @param columns
         * @return
         */
        public int match(String[] columns){
            int score = 0;
            for (int i = 0; i < indexColumns.length && i<columns.length; i++) {
                if(!Objects.equals(indexColumns[i].getName(),columns[i])){
                    break;
                }
                score = i;
            }
            return score;
        }

        public List<IndexRowData> getByPrefix(Object...keys){
            List<IndexRowData> list = new ArrayList<>();
            ConcurrentNavigableMap<Object[], Object[]> subMap = data.prefixSubMap(keys);
            for (Map.Entry<Object[], Object[]> entry : subMap.entrySet()) {
                list.add(parse(entry.getKey(),entry.getValue()));
            }
            return list;
        }

        public IndexRowData parse(Object[] indexValues,Object[] pkValues){
            Map<String,Object> indexValueMap = new LinkedHashMap<>();
            for (int i = 0; i < indexColumns.length; i++) {
                indexValueMap.put(indexColumns[i].getName(),indexValues[i]);
            }

            Map<String,Object> pkValueMap = new LinkedHashMap<>();
            for (int i = 0; i < pkColumns.length; i++) {
                pkValueMap.put(pkColumns[i].getName(),pkValues[i]);
            }

            IndexRowData rowData = new IndexRowData();
            rowData.setPkColumnValues(pkValueMap);
            rowData.setIndexColumnValues(indexValueMap);
            return rowData;
        }

        @Override
        public String toString() {
            return tableName+"."+indexName;
        }
    }

    @Getter
    static class Column{
        private String name;
        private Serializer serializer;
        private Class type;
        public Column(String name, Serializer serializer,Class type) {
            this.name = name;
            this.serializer = Objects.requireNonNull(serializer);
            this.type = Objects.requireNonNull(type);
        }

        public Object cast(Object value){
            Object cast = TypeUtils.cast(value, type, ParserConfig.getGlobalInstance());
            return cast;
        }
        @Override
        public String toString() {
            return name;
        }
    }

}
