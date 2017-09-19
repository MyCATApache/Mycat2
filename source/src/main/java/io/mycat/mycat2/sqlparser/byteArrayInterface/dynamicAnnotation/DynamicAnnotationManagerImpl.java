package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;

import java.util.*;
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
    Map<Integer, DynamicAnnotation[]> cache;

    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath, Map<Integer, DynamicAnnotation[]> cache) throws Exception {
        ActonFactory<BufferSQLContext> actonFactory = new ActonFactory<>(actionsPath);
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
                if (state == null) continue;
                if (!state.trim().toUpperCase().equals("OPEN")) continue;
                SQLType type = SQLType.valueOf(match.getSqltype().toUpperCase().trim());
                DynamicAnnotationKey key = new DynamicAnnotationKey(
                        schemaName,
                        type,
                        match.getTables().toArray(new String[match.getTables().size()]),
                        match.getName());
                List<Map<String, String>> conditionList = match.getWhere();
                Map<Boolean, List<Map<String, String>>> map =
                        conditionList.stream().collect(Collectors.partitioningBy((p) -> {
                            String string = ConditionUtil.mappingKeyInAndOr(p).toUpperCase().trim();
                            return "AND".equals(string);
                        }));
                Map<Boolean, List<String>> resMap = new HashMap<>();
                resMap.put(Boolean.TRUE, map.get(Boolean.TRUE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                resMap.put(Boolean.FALSE, map.get(Boolean.FALSE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                DynamicAnnotationRuntime runtime = DynamicAnnotationUtil.compile(resMap);
                DynamicAnnotationMatch matc = runtime.getMatch();
                System.out.println(Arrays.toString(matc.getCompleteTags()));
                DynamicAnnotation annotation = new DynamicAnnotation(key, runtime.getMatch(), actonFactory.get(match.getActions()), runtime);
                table.put(key, annotation);
            }
        }
        this.route = new DynamicAnnotationKeyRoute(table);
        this.cache = cache;
    }

    public DynamicAnnotation[] getAnnotations(int schema, int sqltype, int[] tables) throws Exception {
        DynamicAnnotation res[];
        DynamicAnnotation[] proto = route.front(schema, sqltype, tables);
        res = new DynamicAnnotation[proto.length];
        for (int i = 0; i < proto.length; i++) {
            DynamicAnnotation it = proto[i];
            //复制带有状态的match
            res[i] = new DynamicAnnotation(it.key, it.match.newInstance(), it.actions, it.runtime);
        }
        return res;
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

    public Runnable process(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = schema << 3 + sqltype << 2 + Arrays.hashCode(tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            cache.put(hash, annotations = getAnnotations(schema, sqltype, tables));
        }
        DynamicAnnotation[] res = annotations;
        return () -> doAnnotations(res, context);
    }

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param
     * @return
     */
    public void processNow(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {
        Arrays.sort(tables);
        DynamicAnnotation[] annotations;
        int hash = schema << 3 + sqltype << 2 + Arrays.hashCode(tables);
        annotations = cache.get(hash);
        if (annotations == null) {
            cache.put(hash, annotations = getAnnotations(schema, sqltype, tables));
        }
        DynamicAnnotation[] res = annotations;
        doAnnotations(res, context);
    }

    private DynamicAnnotationManagerImpl(Map<Integer, DynamicAnnotation[]> cache, DynamicAnnotationKeyRoute route) {
        this.cache = cache;
        this.route = route;
    }

    /**
     * 不同线程复制一个
     *
     * @param cache
     * @return
     */
    public DynamicAnnotationManagerImpl prototype(Map<Integer, DynamicAnnotation[]> cache) {
        return new DynamicAnnotationManagerImpl(cache, this.route);
    }

    public DynamicAnnotationManagerImpl(String actionsPath, String annotationsPath) throws Exception {
        this(actionsPath, annotationsPath, new LinkedHashMap<>(128, 0.75f, true));
    }

    public static void main(String[] args) throws Exception {
        DynamicAnnotationManagerImpl manager = new DynamicAnnotationManagerImpl("actions.yaml", "annotations.yaml");
        BufferSQLContext context = new BufferSQLContext();
        BufferSQLParser sqlParser = new BufferSQLParser();
        String str = "select * where id between 1 and 100 and name = \"haha\" and a=1 and name2 = \"ha\"";
        System.out.println(str);
        sqlParser.parse(str.getBytes(), context);
        manager.prototype(new HashMap<>()).process("schemA", SQLType.INSERT, new String[]{"x1"}, context).run();
    }
}
