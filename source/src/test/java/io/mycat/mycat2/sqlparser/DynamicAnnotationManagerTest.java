package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/15.
 */
public class DynamicAnnotationManagerTest extends TestCase {


    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicAnnotationManagerTest.class);

    //动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
    public static void main(String[] args) throws Exception {
        DynamicAnnotationManager manager=new DynamicAnnotationManager();
        ActonFactory<BufferSQLContext> actonFactory = new ActonFactory<>("actions.yaml");
        RootBean object = YamlUtil.load("annotations.yaml", RootBean.class);
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
//       Map<Boolean, List<Map<String, String>>> map=

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
                DynamicAnnotation annotation=new DynamicAnnotation(key,runtime.getMatch(),actonFactory.get(match.getActions()),manager,runtime);
                table.put(key, annotation);//todo
            }
        }
        System.out.println(table);
        BufferSQLContext context=new BufferSQLContext();
        BufferSQLParser sqlParser=new BufferSQLParser();
        String str="select * where id between 1 and 100 and name = \"haha\" and a=1 and name2 = \"ha\"";
        System.out.println(str);
        sqlParser.parse(str.getBytes(),context);
        DynamicAnnotationKey key=new DynamicAnnotationKey("schemA",SQLType.INSERT,new String[]{"x1","x2","x3"},"aaa");
        table.get(key).match(context);
    }

}