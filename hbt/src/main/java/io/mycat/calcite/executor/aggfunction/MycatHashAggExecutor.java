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
///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.calcite.executor.aggfunction;
//
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//import org.apache.calcite.rel.core.Aggregate;
//
//import java.util.Iterator;
//
//public class MycatHashAggExecutor extends MycatAbstractAggExecutor implements Executor {
//    private Iterator<Row> iter;
//
//
//    public MycatHashAggExecutor(Executor input, Aggregate rel) {
//       super(input,rel);
//    }
//
//    public static MycatHashAggExecutor create(Executor input, Aggregate rel) {
//        return new MycatHashAggExecutor(input, rel);
//    }
//
//    @Override
//    public void open() {
//        if (iter == null) {
//            input.open();
//            Row row = null;
//            while ((row = input.next()) != null) {
//                for (Grouping group : groups) {
//                    group.send(row);
//                }
//            }
//        }
//        this.iter = groups.stream().flatMap(i -> i.end()).iterator();
//    }
//
//    @Override
//    public Row next() {
//        if (iter.hasNext()) {
//            return iter.next();
//        }
//        return null;
//    }
//
//    @Override
//    public void close() {
//        input.close();
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        return true;
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        this.input.explain(writer);
//        return explainWriter.ret();
//    }
//
//
//}