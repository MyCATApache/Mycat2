package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

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
    Map<Integer, RouteMap<List<DynamicAnnotation>>> frontRoute = new HashMap<>();
   Map<Integer, DynamicAnnotation[]> cache = new LinkedHashMap<>(128, 0.75f, true);

    private static int hash(String schema, SQLType sqltype) {
        return hash(schema.hashCode(), sqltype.ordinal());
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
            mapList.compute(hash, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(it.getValue());
                return v;
            });
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
            frontRoute.put(it.getKey(), routeMap);
        }
    }

    public DynamicAnnotation[] front(int schema, int sqltype, int[] tables) {
        DynamicAnnotation[] res;
        int hash=schema << 3 + sqltype << 2 +Arrays.hashCode(tables);
        res = cache.get(hash);
        if (res != null) return res;
        RouteMap<List<DynamicAnnotation>> routeMap = frontRoute.get(hash(schema, sqltype));
        if (routeMap != null) {
            res= routeMap.get(tables).stream().flatMap((v) -> v.stream()).distinct().toArray(DynamicAnnotation[]::new);
            cache.put(hash,res);
            return res;
        } else {
            //todo 错误
            return null;
        }
    }

    public DynamicAnnotation[] front(String schema, SQLType sqltype, int[] tables) {
        return front(schema.hashCode(), sqltype.ordinal(), tables);
    }


}
