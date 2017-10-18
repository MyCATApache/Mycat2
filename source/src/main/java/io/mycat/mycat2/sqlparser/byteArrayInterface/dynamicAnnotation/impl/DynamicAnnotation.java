package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jamie on 2017/9/5.
 */

public class DynamicAnnotation {
 public final DynamicAnnotationKey key;
 public final DynamicAnnotationMatch match;
 public final SQLAnnotationList actions;
 public final DynamicAnnotationRuntime runtime;
    public final List<Map<String, Map<String, String>>> globalActions;
    public final List<Map<String, Map<String, String>>> schemaActions;
    
    private static final Logger logger = LoggerFactory.getLogger(DynamicAnnotation.class);

    public DynamicAnnotation(DynamicAnnotationKey key, DynamicAnnotationMatch match, SQLAnnotationList actions, DynamicAnnotationRuntime runtime, List<Map<String, Map<String, String>>> globalActions, List<Map<String, Map<String, String>>> schemaActions) {
        this.key = key;
        this.match = match;
        this.actions = actions;
        this.runtime = runtime;
        this.globalActions = globalActions;
        this.schemaActions = schemaActions;
    }

    public void reduce(Map<String, SQLAnnotation> schemaActionsMap, Map<String, SQLAnnotation> globalActionsMap) throws Exception {
        HashSet<String> set = new HashSet<>();
        List<SQLAnnotation> list=actions.getSqlAnnotations();
 //       int size=list.size();
        //后面的的覆盖(替代)前面的
//        for(int i=size-1;i>-1;i--){
//            String method =list.get(i).getActionName();
//            if (method == null) {
//                logger.warn("=> action名字为null.{},跳过此action:", method);
//                continue;
//            }
//            if (!set.contains(method)) {
//                set.add(method);
//            } else {
//              //  logger.warn("=> action名字重复了.{}",method);
//               // list.remove(i);
//            }
//        }

        actions.getSqlAnnotations().addAll(0,reduceHelper(set, schemaActions,  schemaActionsMap));
        actions.getSqlAnnotations().addAll(0,reduceHelper(set, globalActions,  globalActionsMap));
    }

    private static List<SQLAnnotation> reduceHelper(HashSet<String> set, List<Map<String, Map<String, String>>> list, Map<String, SQLAnnotation> provider) throws Exception {
        if (list==null){
            return Collections.EMPTY_LIST;
        }
        List<SQLAnnotation> res=new ArrayList<>();
        Iterator<Map<String, Map<String, String>>> schemaIter = list.iterator();
        while (schemaIter.hasNext()) {
            Map<String, Map<String, String>> it = schemaIter.next();
            String method = ConditionUtil.mappingKey(it);
           // if (!set.contains(method)) {
               SQLAnnotation sqlAnnotation = provider.get(method);
                res.add(sqlAnnotation);
            //}
        }
        return res;
    }

    @Override
    public String toString() {
        return "DynamicAnnotation{" +
                "key=" + key +
                ", match=" + match +
                ", actions=" + actions +
                ", runtime=" + runtime +
                ", globalActions=" + globalActions +
                ", schemaActions=" + schemaActions +
                '}';
    }
}
