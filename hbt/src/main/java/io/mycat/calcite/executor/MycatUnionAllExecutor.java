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
//package io.mycat.calcite.executor;
//
//
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//
//public class MycatUnionAllExecutor implements Executor {
//    int index = 0;
//    final Executor[] executors;
//
//    protected MycatUnionAllExecutor(Executor[] executors) {
//        this.executors = executors;
//    }
//
//    public static MycatUnionAllExecutor create(Executor[] executors) {
//        return new MycatUnionAllExecutor(executors);
//    }
//
//    @Override
//    public void open() {
//        for (Executor executor : executors) {
//            executor.open();
//        }
//    }
//
//    @Override
//    public Row next() {
//        if (index < executors.length) {
//            Executor executor = executors[index];
//            Row row = executor.next();
//            if (row == null) {
//                executor.close();
//                index++;
//                if (index >= executors.length) {
//                    return null;
//                } else {
//                    return next();
//                }
//            }
//            return row;
//        } else {
//            return null;
//        }
//    }
//
//    @Override
//    public void close() {
//        for (Executor executor : executors) {
//            executor.close();
//        }
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        return false;
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        for (Executor executor : executors) {
//            executor.explain(writer);
//        }
//        return explainWriter.ret();
//    }
//}