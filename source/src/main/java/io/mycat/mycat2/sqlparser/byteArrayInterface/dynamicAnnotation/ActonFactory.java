package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;


import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/10.
 */
public class ActonFactory<T> {
    String config = null;
    HashMap<String, Class<SQLAnnotation<T>>> resMap;


    public Function<T,T> get(List<Map<String, List<Map<String, String>>>> need) throws Exception {
        Iterator<Map<String,  List<Map<String, String>>>> iterator = need.iterator();
        Function<T,T> res = null;
        do {
            Map<String,  List<Map<String, String>>> action = iterator.next();
            Map.Entry<String,  List<Map<String, String>>> entry = action.entrySet().iterator().next();
            String actionName = entry.getKey();
           System.out.println( entry.toString());
            Class<SQLAnnotation<T>> annotationClass = resMap.get(actionName);
            SQLAnnotation<T> annotation = annotationClass.newInstance();
            Optional.ofNullable(entry.getValue()).map(s->s.stream().collect(Collectors.toMap(ConditionUtil::mappingKey,(v)->{
                if (v==null){
                    return null;
                }else {
                    return ConditionUtil.mappingValue(v);
                }
            }))).ifPresent((args->  annotation.init(args)));
            if (res == null) {
                res = annotation;
            } else {
                res = res.andThen(annotation);
            }
        } while (iterator.hasNext());
        return res==null?EMPTY:res;
    }

    public static void main(String[] args) throws Throwable {
        ActonFactory<BufferSQLContext> actonFactory = new ActonFactory<>("actions.yaml");
        List<Map<String,List< Map<String, String>>>> list = new ArrayList<>();
        Map<String,String> monitorSQL=new HashMap<>();
        monitorSQL.put("param1","1");
        list.add(Collections.singletonMap("monitorSQL",Collections.singletonList(monitorSQL)));
        list.add(Collections.singletonMap("cacheResult",null));
        Map<String,String> sqlCach=new HashMap<>();
        sqlCach.put("param1","1");
        sqlCach.put("param2","2");
        list.add(Collections.singletonMap("sqlCach",Arrays.asList(sqlCach)));
       Function<BufferSQLContext,BufferSQLContext> annotations= actonFactory.get(list);
       annotations.apply(null);
    }

    public ActonFactory(String config) throws Exception {
        this.config = config.trim();
        URL url = ActonFactory.class.getClassLoader().getResource(config);
        if (url != null) {
            Yaml yaml = new Yaml();
            FileInputStream fis = new FileInputStream(url.getFile());
            Map<String, List<Map<String, String>>> obj = (Map<String, List<Map<String, String>>>) yaml.load(fis);
            Map<String, String> actions = obj.values().stream().flatMap(maps -> maps.stream()).collect(Collectors.toMap(ConditionUtil::mappingKey, ConditionUtil::mappingValue));
            resMap = new HashMap<>(actions.size());
            for (Map.Entry<String, String> it : actions.entrySet()) {
                String value = it.getValue();
                if (value == null) continue;
                Class<SQLAnnotation<T>> k = (Class<SQLAnnotation<T>>) ActonFactory.class.getClassLoader().loadClass(value.trim());
                resMap.put(it.getKey().trim(), k);
            }
        }
    }

    public String getConfig() {
        return config;
    }
    final  SQLAnnotation<T> EMPTY=new SQLAnnotation<T>() {

        @Override
        public void init(Map<String, String> atgs) {

        }

        @Override
        public T apply(T t) {
            return t;
        }
    };
}
