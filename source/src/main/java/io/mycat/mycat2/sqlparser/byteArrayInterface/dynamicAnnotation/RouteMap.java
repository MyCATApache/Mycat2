package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import java.util.*;

/**
 * Created by jamie on 2017/9/16.
 */
public class RouteMap<T> {
    final HashMap<Integer, List<T>> map;

    public List<T> get(int[] targets) {
        int size = targets.length;
        List<T> c = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            List<T> res = map.get(targets[i]);
            int s = res.size();
            for (int j = 0; j < s; j++) {
                T v = res.get(j);
                c.add(v);
            }
        }
        return c;
    }


    public RouteMap(Map<int[], T> map) {
        HashMap<Integer, List<T>> tmp = new HashMap<>();
        Iterator<Map.Entry<int[], T>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<int[], T> entry = iterator.next();
            int[] key = entry.getKey();
            Arrays.sort(key);
            for (int j = 0; j < key.length; j++) {
                tmp.compute(key[j], (k, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    list.add(entry.getValue());
                    return list;
                });
            }
        }
        this.map = tmp;
    }
}
