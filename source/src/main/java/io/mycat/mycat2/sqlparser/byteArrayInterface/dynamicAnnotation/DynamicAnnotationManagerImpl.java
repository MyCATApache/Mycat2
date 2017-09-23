package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    final Map<Integer, List<SQLAnnotationList>> globalFunction = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(DynamicAnnotationManagerImpl.class);
    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath, Map<Integer, DynamicAnnotation[]> cache) throws Exception {
       try {
           ActonFactory actonFactory = new ActonFactory(actionsPath);
           RootBean object = YamlUtil.load(annotationsPath, RootBean.class);
           HashMap<DynamicAnnotationKey, DynamicAnnotation> table = new HashMap<>();
           Iterator<Schema> iterator = object.getAnnotations().stream().map((s) -> s.getSchema()).iterator();
           while (iterator.hasNext()) {
               Schema schema = iterator.next();
               String schemaName = schema.getName().trim();
               List<Matches> matchesList = schema.getMatches();
               for (Matches matche : matchesList) {
                   Match match = matche.getMatch();
                   String state = match.getState();
                   if (state == null) {
                       logger.error("state == null");
                       continue;
                   }
                   if (!state.trim().toUpperCase().equals("OPEN")) {
                       continue;
                   }
                   if (match.getActions() == null || match.getActions().isEmpty()) {
                       logger.error("actions == null");
                       continue;
                   }
                   if (match.getSqltype() == null) {
                       logger.error("sqltype == null");
                       continue;
                   }
                   SQLType type = SQLType.valueOf(match.getSqltype().toUpperCase().trim());
                   if (match.getTables() == null) {
                       match.setTables(Collections.EMPTY_LIST);
                   }
                   DynamicAnnotationKey key = new DynamicAnnotationKey(
                           schemaName,
                           type,
                           match.getTables().toArray(new String[match.getTables().size()]),
                           match.getName());
                   List<Map<String, String>> conditionList = match.getWhere();
                   if (conditionList == null || conditionList.isEmpty()||match.getTables().isEmpty()) {
                       globalFunction.compute(globalFunctionHash(schemaName.hashCode(), type.getValue()), (k, v) -> {
                           if (v == null) {
                               v = new ArrayList<>();
                           }
                           try {
                               v.add(actonFactory.get(match.getName(), match.getActions()));
                           } catch (Exception e) {
                               e.printStackTrace();
                           }
                           return v;
                       });
                   } else {
                       Map<Boolean, List<Map<String, String>>> map =
                               conditionList.stream().collect(Collectors.partitioningBy((p) -> {
                                   String string = ConditionUtil.mappingKeyInAndOr(p).toUpperCase().trim();
                                   return "AND".equals(string);
                               }));
                       Map<Boolean, List<String>> resMap = new HashMap<>();
                       resMap.put(Boolean.TRUE, map.get(Boolean.TRUE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                       resMap.put(Boolean.FALSE, map.get(Boolean.FALSE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                       DynamicAnnotationRuntime runtime = DynamicAnnotationUtil.compile(match.getName(),resMap);
                       DynamicAnnotationMatch matc = runtime.getMatch();
                       System.out.println(Arrays.toString(matc.getCompleteTags()));
                       DynamicAnnotation annotation = new DynamicAnnotation(key, runtime.getMatch(), actonFactory.get(match.getName(), match.getActions()), runtime);
                       table.put(key, annotation);
                   }
               }
           }

           this.route = new DynamicAnnotationKeyRoute(table);
           this.cache = cache;
       }catch (Exception e){
           e.printStackTrace();
           logger.error("动态注解语法错误");
           throw e;
       }
    }

    public DynamicAnnotation[] getAnnotations(int schema, int sqltype, int[] tables) throws Exception {
        DynamicAnnotation[] proto = route.front(schema, sqltype, tables);
        return proto;
    }

    public static void doAnnotations(DynamicAnnotation[] res, BufferSQLContext context) {
        int size = res.length;
        for (int i = 0; i < size; i++) {
            DynamicAnnotation annotation = res[i];
            try {
                annotation.match.pick(0, context);
                if (annotation.match.isComplete()) {
                    annotation.actions.apply(context);
                }
            }catch (Exception e){
                System.out.println(annotation.toString());
                e.printStackTrace();
            }
        }
    }

    public static void collectAnnotations(DynamicAnnotation[] res, BufferSQLContext context, List<SQLAnnotationList> list) {
        int size = res.length;
        for (int i = 0; i < size; i++) {
            DynamicAnnotation annotation = res[i];
            try {
                annotation.match.pick(0, context);
                if (annotation.match.isComplete()) {
                    list.add(annotation.actions);
                }
            } catch (Exception e) {
                System.out.println(annotation.toString());
                e.printStackTrace();
            }
        }
    }

    public List<SQLAnnotationList> getGlobalFunctionAnnotations(int schema, int sqltype) {
        int hash = globalFunctionHash(schema, sqltype);
        List<SQLAnnotationList> list = this.globalFunction.get(hash);
        return list;
    }


    public Runnable process(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = hash(schema, sqltype, tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            annotations = getAnnotations(schema, sqltype, tables);
            if (annotations == null) {

            } else {
                cache.put(hash, annotations);
            }

        }
        DynamicAnnotation[] res = annotations;
        List<SQLAnnotationList> globalFunction = getGlobalFunctionAnnotations(schema, sqltype);
        if (res == null && globalFunction == null) {
            return () -> {
                logger.debug("没有匹配的action");
            };
        } else if (res != null && globalFunction == null) {
            return () -> {
                doAnnotations(res, context);
            };
        } else if (res == null && globalFunction != null) {
            return () -> {
                doList(globalFunction, context);
            };
        }
        return () -> {
            doList(globalFunction, context);
            doAnnotations(res, context);
        };
    }

    public void collect(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = hash(schema, sqltype, tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            annotations = getAnnotations(schema, sqltype, tables);
            if (annotations == null) {

            } else {
                cache.put(hash, annotations);
            }

        }
        DynamicAnnotation[] res = annotations;
        List<SQLAnnotationList> globalFunction = getGlobalFunctionAnnotations(schema, sqltype);
        if (res == null && globalFunction == null) {

        } else if (res != null && globalFunction == null) {
            collectAnnotations(res, context, collect);
        } else if (res == null && globalFunction != null) {
            int size = globalFunction.size();
            for (int i = 0; i < size; i++) {
                collect.add(globalFunction.get(i));
            }
        }else {
            int size = globalFunction.size();
            for (int i = 0; i < size; i++) {
                collect.add(globalFunction.get(i));
            }
            collectAnnotations(res, context, collect);
        }

    }

    private static void doList(List<SQLAnnotationList> globalFunction, BufferSQLContext args) {
        int size = globalFunction.size();
        for (int i = 0; i < size; i++) {
            globalFunction.get(i).apply(args);
        }
    }

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param
     * @return
     */
//    public void processNow(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
//        Arrays.sort(tables);
//        DynamicAnnotation[] annotations;
//        int hash = hash(schema, sqltype, tables);
//        annotations = cache.get(hash);
//        if (annotations == null) {
//            cache.put(hash, annotations = getAnnotations(schema, sqltype, tables));
//        }
//        DynamicAnnotation[] res = annotations;
//        List<Function<BufferSQLContext, BufferSQLContext>> globalFunction = getGlobalFunctionAnnotations(schema, sqltype);
//        doList(globalFunction, context);
//        doAnnotations(res, context);
//    }

    private DynamicAnnotationManagerImpl(Map<Integer, DynamicAnnotation[]> cache, DynamicAnnotationKeyRoute route) {
        this.cache = cache;
        this.route = route;
    }

    private static int hash(int schema, int sqltype, int[] tables) {
        int hash = schema;
        hash = hash * 31 + sqltype;
        hash = hash * 31 + Arrays.hashCode(tables);
        return hash;
    }

    private static int globalFunctionHash(int schema, int sqltype) {
        int hash = schema;
        System.out.println(schema);
        hash = hash * 31 + sqltype;
        System.out.println(sqltype);
        System.out.println("globalFunctionHash:" + hash);
        return hash;
    }


    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath) throws Exception {
        this(actionsPath, annotationsPath, new ConcurrentHashMap<>());
    }

    public static void main(String[] args) throws Exception {
        DynamicAnnotationManagerImpl manager = new DynamicAnnotationManagerImpl("actions.yaml", "annotations.yaml");
        BufferSQLContext context = new BufferSQLContext();
        BufferSQLParser sqlParser = new BufferSQLParser();
        String str = "select * where id between 1 and 100 and name = \"haha\" and a=1 and name2 = \"ha\"";
        System.out.println(str);
        sqlParser.parse(str.getBytes(), context);
        manager.process("schemA", SQLType.INSERT, new String[]{"x1"}, context).run();
    }
}
