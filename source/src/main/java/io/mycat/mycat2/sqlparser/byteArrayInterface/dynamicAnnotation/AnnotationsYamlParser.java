package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.*;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/24.
 */
public class AnnotationsYamlParser {
    private static final Logger logger = LoggerFactory.getLogger(DynamicAnnotationManagerImpl.class);

    public static void main(String[] args) {

    }

    public static Map<DynamicAnnotationKey, DynamicAnnotation> parse(String annotationsPath,
                                                                     ActonFactory actonFactory,
                                                                     Map<Integer, List<SQLAnnotationList>> schemaWithSQLtypeFunction) throws Exception {
        RootBean object = YamlUtil.load(annotationsPath, RootBean.class);
        HashMap<DynamicAnnotationKey, DynamicAnnotation> table = new HashMap<>();

        List<Annotations> annotations = object.getAnnotations().stream().filter((i) -> i.getGlobal() == null).collect(Collectors.toList());
        List<Annotations> global = object.getAnnotations().stream().filter((i) -> i.getGlobal() != null).collect(Collectors.toList());
        List<Map<String, Map<String, String>>> globalFun;
        if (global.size() == 1) {
            globalFun = global.get(0).getGlobal();
        } else {
            globalFun = Collections.EMPTY_LIST;
        }
        Map<String, SQLAnnotation> globalActionList = scopeActionHelper("global", globalFun, actonFactory);
        Iterator<Schema> iterator = annotations.stream().map((s) -> s.getSchema()).iterator();
        while (iterator.hasNext()) {
            Schema schema = iterator.next();
            String schemaName = schema.getName().trim();
            List<Matches> matchesList = schema.getMatches();
            Map<String, SQLAnnotation> schemaActionsList = scopeActionHelper(schemaName, schema.blacklist, actonFactory);
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
                if (conditionList == null || conditionList.isEmpty() || match.getTables().isEmpty()) {
                    Integer ke = DynamicAnnotationManagerImpl.getGlobalFunctionHash(schemaName.hashCode(), type.getValue());
                    schemaWithSQLtypeFunction.compute(ke, (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        try {
                            //注意顺序
                            SQLAnnotationList list = actonFactory.get(match.getName(), match.getActions());//最后
                            list.getSqlAnnotations().addAll(0, schemaActionsList.values());//中间
                            list.getSqlAnnotations().addAll(0, globalActionList.values());//最前
                            v.add(list);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return v;
                    });
                    //处理前置类
                } else {
                    Map<Boolean, List<Map<String, String>>> map =
                            conditionList.stream().collect(Collectors.partitioningBy((p) -> {
                                String string = ConditionUtil.mappingKeyInAndOr(p).toUpperCase().trim();
                                return "AND".equals(string);
                            }));
                    Map<Boolean, List<String>> resMap = new HashMap<>();
                    resMap.put(Boolean.TRUE, map.get(Boolean.TRUE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                    resMap.put(Boolean.FALSE, map.get(Boolean.FALSE).stream().map((m) -> ConditionUtil.mappingValue(m)).distinct().collect(Collectors.toList()));
                    DynamicAnnotationRuntime runtime = DynamicAnnotationUtil.compile(match.getName(), resMap);
                    DynamicAnnotationMatch matc = runtime.getMatch();
                    System.out.println(Arrays.toString(matc.getCompleteTags()));

                    DynamicAnnotation annotation = new DynamicAnnotation(key,
                            runtime.getMatch(),
                            actonFactory.get(match.getName(), match.getActions())
                            , runtime, globalFun, schema.blacklist);
                    annotation.reduce(schemaActionsList, globalActionList);
                    table.put(key, annotation);
                }
            }
        }
        return table;
    }

    private static Map<String, SQLAnnotation> scopeActionHelper(String matchName, List<Map<String, Map<String, String>>> list, ActonFactory actonFactory) {
        return list.stream().collect(Collectors.toMap((g) -> ConditionUtil.mappingKey(g), (g) -> {
            try {
                String name = ConditionUtil.mappingKey(g);
                return actonFactory.getActionByActionName(name, g.get(name), matchName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }));
    }

}
