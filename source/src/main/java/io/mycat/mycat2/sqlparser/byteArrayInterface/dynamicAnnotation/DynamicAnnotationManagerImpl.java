package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.ActonFactory;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotation;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationKeyRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/15.
 */
public class DynamicAnnotationManagerImpl implements DynamicAnnotationManager {
    //  AnnotationSchemaList annotations;
    public static final String annotation_list = "annotations";
    public static final String schema_tag = "schema_tag";
    public static final String schema_name = "name";
    public static final String match_list = "matches";
    public static final String match_tag = "match";
    public static final String match_name = "name";
    public static final String match_state = "state";
    public static final String match_sqltype = "sqltype";
    public static final String match_where = "where";
    public static final String match_tables = "tables";
    public static final String match_actions = "actions";
    final DynamicAnnotationKeyRoute route;
    final Map<Integer, DynamicAnnotation[]> cache;
    final Map<Integer, List<SQLAnnotationList>> schemaWithSQLtypeFunction = new HashMap<>();
    SQLAnnotationList globalSQLAnnotationList = null;
    private static final Logger logger = LoggerFactory.getLogger(DynamicAnnotationManagerImpl.class);
    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath, Map<Integer, DynamicAnnotation[]> cache) throws Exception {
       try {
           ActonFactory actonFactory = new ActonFactory(actionsPath);
           Map<String, SQLAnnotation> globalActionList = new HashMap<>();
           this.route = new DynamicAnnotationKeyRoute(AnnotationsYamlParser.parse(annotationsPath, actonFactory, schemaWithSQLtypeFunction, globalActionList));
           SQLAnnotationList global = new SQLAnnotationList();
           global.setSqlAnnotations(globalActionList.values().stream().collect(Collectors.toList()));
           globalSQLAnnotationList = global;
           this.cache = cache;
       }catch (Exception e){
           e.printStackTrace();
           logger.error("动态注解语法错误");
           throw e;
       }
    }

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param
     * @return
     */
//    public void processNow(int mycatSchema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
//        Arrays.sort(tables);
//        DynamicAnnotation[] annotations;
//        int getHash = getHash(mycatSchema, sqltype, tables);
//        annotations = cache.get(getHash);
//        if (annotations == null) {
//            cache.put(getHash, annotations = getAnnotations(mycatSchema, sqltype, tables));
//        }
//        DynamicAnnotation[] res = annotations;
//        List<Function<BufferSQLContext, BufferSQLContext>> schemaWithSQLtypeFunction = getSchemaWithSQLtypeFunction(mycatSchema, sqltype);
//        doList(schemaWithSQLtypeFunction, context);
//        doAnnotations(res, context);
//    }
    private DynamicAnnotationManagerImpl(Map<Integer, DynamicAnnotation[]> cache, DynamicAnnotationKeyRoute route) {
        this.cache = cache;
        this.route = route;
    }

    public static void doAnnotations(DynamicAnnotation[] res, BufferSQLContext context) {
        int size = res.length;
        for (int i = 0; i < size; i++) {
            DynamicAnnotation annotation = res[i];
            try {
                annotation.match.pick(0, context);
                if (annotation.match.isComplete()) {
                	logger.debug(annotation.actions.getSqlAnnotations().toString());
                }
            }catch (Exception e){
            	logger.error(annotation.toString());
                e.printStackTrace();
            }
        }
    }

    public static void collectAnnotationsListSQLAnnotationList(DynamicAnnotation[] res, BufferSQLContext context, List<SQLAnnotationList> list) {
        int size = res.length;
        for (int i = 0; i < size; i++) {
            DynamicAnnotation annotation = res[i];
            try {
                annotation.match.pick(0, context);
                if (annotation.match.isComplete()) {
                    list.add(annotation.actions);
                }
            } catch (Exception e) {
            	logger.error(annotation.toString());
                e.printStackTrace();
            }
        }
    }
    public static void collectAnnotationsListSQLAnnotation(DynamicAnnotation[] res, BufferSQLContext context, List<SQLAnnotation> list) {
        int size = res.length;
        for (int i = 0; i < size; i++) {
            DynamicAnnotation annotation = res[i];
            try {
                annotation.match.pick(0, context);
                if (annotation.match.isComplete()) {
                    list.addAll(annotation.actions.getSqlAnnotations());
                }
            } catch (Exception e) {
            	logger.error(annotation.toString());
                e.printStackTrace();
            }
        }
    }

    public List<SQLAnnotationList> getSchemaWithSQLtypeFunction(int schema, int sqltype) {
        int hash = getGlobalFunctionHash(schema, sqltype);
        List<SQLAnnotationList> list = this.schemaWithSQLtypeFunction.get(hash);
        return list;
    }


    public Runnable process(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = getHash(schema, sqltype, tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            annotations = getAnnotations(schema, sqltype, tables);
            if (annotations == null) {

            } else {
                cache.put(hash, annotations);
            }

        }
        DynamicAnnotation[] res = annotations;
        List<SQLAnnotationList> schemaWithSQLtypeFunction = getSchemaWithSQLtypeFunction(schema, sqltype);
        if (res == null && schemaWithSQLtypeFunction == null) {
            return () -> {
                logger.debug("没有匹配的action");
            };
        } else if (res != null && schemaWithSQLtypeFunction == null) {
            return () -> {
                doAnnotations(res, context);
            };
        } else if (res == null && schemaWithSQLtypeFunction != null) {
            return () -> {
                doList(schemaWithSQLtypeFunction, context);
            };
        }
        return () -> {
            doAnnotations(res, context);
            doList(schemaWithSQLtypeFunction, context);
        };
    }

    public void collectInSQLAnnotationList(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = getHash(schema, sqltype, tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            annotations = getAnnotations(schema, sqltype, tables);
            if (annotations == null) {

            } else {
                cache.put(hash, annotations);
            }

        }
        DynamicAnnotation[] res = annotations;
        List<SQLAnnotationList> schemaWithSQLtypeFunction = getSchemaWithSQLtypeFunction(schema, sqltype);
        collect.add(globalSQLAnnotationList);
        int size;
        if (res == null && schemaWithSQLtypeFunction == null) {

        } else if (res != null && schemaWithSQLtypeFunction == null) {
            collectAnnotationsListSQLAnnotationList(res, context, collect);
        } else if (res == null && schemaWithSQLtypeFunction != null) {
            size = schemaWithSQLtypeFunction.size();
            for (int i = 0; i < size; i++) {
                collect.add(schemaWithSQLtypeFunction.get(i));
            }
        }else {
            collectAnnotationsListSQLAnnotationList(res, context, collect);
            size = schemaWithSQLtypeFunction.size();
            for (int i = 0; i < size; i++) {
                collect.add(schemaWithSQLtypeFunction.get(i));
            }
        }

    }
    public void collect(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotation> collect) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = getHash(schema, sqltype, tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            annotations = getAnnotations(schema, sqltype, tables);
            if (annotations == null) {

            } else {
                cache.put(hash, annotations);
            }

        }
        DynamicAnnotation[] res = annotations;
        int size=globalSQLAnnotationList.getSqlAnnotations().size();
        for (int i = 0; i <size ; i++) {
            collect.add(globalSQLAnnotationList.getSqlAnnotations().get(i));
        }
        List<SQLAnnotationList> globalFunction = getSchemaWithSQLtypeFunction(schema, sqltype);
        if (res == null && globalFunction == null) {

        } else if (res != null && globalFunction == null) {
            collectAnnotationsListSQLAnnotation(res, context, collect);
        } else if (res == null && globalFunction != null) {
            size = globalFunction.size();
            for (int i = 0; i < size; i++) {
                collect.addAll(globalFunction.get(i).getSqlAnnotations());
            }
        }else {
            size = globalFunction.size();
            for (int i = 0; i < size; i++) {
                collect.addAll(globalFunction.get(i).getSqlAnnotations());
            }
            collectAnnotationsListSQLAnnotation(res, context, collect);
        }

    }
    private static void doList(List<SQLAnnotationList> globalFunction, BufferSQLContext args) {
        int size = globalFunction.size();
        for (int i = 0; i < size; i++) {
        	logger.debug(globalFunction.get(i).getSqlAnnotations().toString());
        }
    }

    public static int getGlobalFunctionHash(int schema, int sqltype) {
        int hash = schema;
        hash = hash * 31 + sqltype;
        logger.debug("mycatSchema is {},sqltype is {},hash is {}", schema, sqltype, hash);
        return hash;
    }

    public static int getHash(int schema, int sqltype, int[] tables) {
        int hash = schema;
        hash = hash * 31 + sqltype;
        hash = hash * 31 + Arrays.hashCode(tables);
        return hash;
    }

    public DynamicAnnotation[] getAnnotations(int schema, int sqltype, int[] tables) {
        DynamicAnnotation[] proto = route.front(schema, sqltype, tables);
        return proto;
    }


    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath) throws Exception {
        this(actionsPath, annotationsPath, new ConcurrentHashMap<>());
    }

    public static void main(String[] args) throws Exception {
        DynamicAnnotationManagerImpl manager = new DynamicAnnotationManagerImpl("actions.yml", "annotations.yml");
        BufferSQLContext context = new BufferSQLContext();
        BufferSQLParser sqlParser = new BufferSQLParser();
        String str = "select * where id between 1 and 100 and name = \"haha\" and a=1 and name2 = \"ha\"";
        System.out.println(str);
        sqlParser.parse(str.getBytes(), context);
        //  INSERT(14), DELETE(13), REPLACE(15), SELECT(11), UPDATE(12);
        manager.process("schemA",12 , new String[]{"x1"}, context).run();
    }
}
