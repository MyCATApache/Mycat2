package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by jamie on 2017/9/15.
 */
public class DynamicAnnotationKeyRoute {
    Map<Integer, RouteMap<List<DynamicAnnotation>>> tablesRoute = new HashMap<>();
    Map<Integer, List<DynamicAnnotation>> notablesRoute = new HashMap<>();
    private static int hash(String schema, SQLType sqltype) {
        return hash(schema.hashCode(), sqltype.getValue());
    }

    private static int hash(int schema, int sqltype) {
        int hash = schema * 31;
        hash = sqltype * 31 + hash;
        return hash;
    }

    public static int[] stringArray2HashArray(String[] v) {
        BufferSQLContext context = new BufferSQLContext();
        BufferSQLParser parser = new BufferSQLParser();
        List<Integer> tabl = Stream.of(v).map((c) ->
        {
            parser.parse(c.getBytes(StandardCharsets.UTF_8), context);
            return context.getHashArray().getIntHash(0);
        }).sorted().collect(Collectors.toList());
        int[] tables = new int[tabl.size()];
        for (int i = 0; i < tabl.size(); i++) {
            tables[i] = tabl.get(i);
        }
        return tables;
    }

    public DynamicAnnotationKeyRoute(Map<DynamicAnnotationKey, DynamicAnnotation> map) {
        HashMap<Integer, List<DynamicAnnotation>> mapList = new HashMap<>();
        for (Map.Entry<DynamicAnnotationKey, DynamicAnnotation> it : map.entrySet()) {
            int hash = hash(it.getKey().schemaName, it.getKey().sqlType);
            String[] tables = it.getKey().tables;
            if (tables == null || tables.length == 0) {//处理无tables的路由
                notablesRoute.compute(hash, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(it.getValue());
                    return v;
                });
            } else {
                mapList.compute(hash, (k, v) -> {//处理有tables的路由
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(it.getValue());
                    return v;
                });
            }
        }
        for (Map.Entry<Integer, List<DynamicAnnotation>> it : mapList.entrySet()) {
            HashMap<int[], List<DynamicAnnotation>> m = new HashMap<>();
            List<DynamicAnnotation> list = it.getValue();
            for (DynamicAnnotation dynamicAnnotation : list) {
                int[] hash = stringArray2HashArray(dynamicAnnotation.key.tables);
                m.compute(hash, (k, v) -> {
                    if (v == null) v = new ArrayList<>();
                    v.add(dynamicAnnotation);
                    return v;
                });
            }
            RouteMap<List<DynamicAnnotation>> routeMap = new RouteMap<List<DynamicAnnotation>>(m);
            tablesRoute.put(it.getKey(), routeMap);
        }
    }

    public DynamicAnnotation[] front(int schema, int sqltype, int[] tables) {
        DynamicAnnotation[] res;
        int hash = hash(schema, sqltype);
        RouteMap<List<DynamicAnnotation>> routeMap = tablesRoute.get(hash(schema, sqltype));
        List<DynamicAnnotation> notablesActions = notablesRoute.get(hash);
        if (notablesActions == null) notablesActions = Collections.emptyList();
        if (routeMap != null) {
            List<List<DynamicAnnotation>> list = routeMap.get(tables);
            if (list == null) list = new ArrayList<>();
            list.add(notablesActions);
            res = list.stream().flatMap((v) -> v.stream()).distinct().toArray(DynamicAnnotation[]::new);
            return res;
        } else {
            //todo 错误
            return null;
        }
    }

    public DynamicAnnotation[] front(String schema, SQLType sqltype, int[] tables) {
        return front(schema.hashCode(), sqltype.getValue(), tables);
    }


}
