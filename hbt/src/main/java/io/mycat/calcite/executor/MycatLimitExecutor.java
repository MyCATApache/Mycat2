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
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//
//import java.util.Iterator;
//import java.util.stream.StreamSupport;
//
//
//public class MycatLimitExecutor implements Executor {
//    private final Executor input;
//    private long offset;
//    private long fetch;
//    private Iterator<Row> iterator;
//
//    protected MycatLimitExecutor(long offset, long fetch, Executor input) {
//        this.offset = offset;
//        this.fetch = fetch;
//        this.input = input;
//    }
//
//    public static MycatLimitExecutor create(long offset, long fetch, Executor input) {
//        return new MycatLimitExecutor(offset, fetch, input);
//    }
//
//    @Override
//    public void open() {
//        input.open();
//        this.iterator = StreamSupport.stream(input.spliterator(), false).skip(offset).limit(fetch).iterator();
//    }
//
//    @Override
//    public Row next() {
//        if (iterator.hasNext()) {
//            return iterator.next();
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
//        return false;
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        explainWriter.item("offset",offset);
//        explainWriter.item("fetch",fetch);
//        input.explain(writer);
//        return explainWriter.ret();
//    }
//}