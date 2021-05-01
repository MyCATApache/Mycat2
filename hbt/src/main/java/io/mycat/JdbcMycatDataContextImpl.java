///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to you under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.mycat;
//
//import io.mycat.calcite.CodeExecuterContext;
//import io.reactivex.rxjava3.core.Observable;
//import org.apache.calcite.linq4j.Enumerable;
//import org.apache.calcite.linq4j.Linq4j;
//import org.apache.calcite.linq4j.function.Function1;
//import org.apache.calcite.rel.RelNode;
//import org.slf4j.LoggerFactory;
//
//import java.util.Comparator;
//import java.util.IdentityHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//public class JdbcMycatDataContextImpl extends NewMycatDataContextImpl {
//    private final Map<String, List<Enumerable<Object[]>>> viewMap;
//    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NewMycatDataContextImpl.class);
//
//    public JdbcMycatDataContextImpl(MycatDataContext dataContext,
//                                    CodeExecuterContext context,
//                                    Map<String, List<Enumerable<Object[]>>> viewMap,
//                                    List<Object> params,
//                                    boolean forUpdate) {
//        super(dataContext, context, params, forUpdate);
//        this.viewMap = new IdentityHashMap<>();
//        Map<String, Integer> mycatViews = codeExecuterContext.getMycatViews();
//        for (Map.Entry<String, List<Enumerable<Object[]>>> entry : viewMap.entrySet()) {
//            String key = entry.getKey();
//            List<Enumerable<Object[]>> observableList = entry.getValue().stream().map(i -> {
//                if (mycatViews.get(key) > 1) {
//                    return Linq4j.asEnumerable(i.toList());
//                } else {
//                    return i;
//                }
//            }).collect(Collectors.toList());
//            this.viewMap.put(key, observableList);
//        }
//    }
//
//    /**
//     * 获取mycatview的迭代器
//     *
//     * @param node
//     * @return
//     */
//    @Override
//    public Enumerable<Object[]> getEnumerable(String node) {
//        List<Enumerable<Object[]>> enumerables = viewMap.get(node);
//        if (enumerables.size() == 1) {
//            return enumerables.get(0);
//        }
//        return Linq4j.concat(enumerables);
//    }
//
//
//
//
//
//}
