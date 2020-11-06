package io.mycat.api.collector;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetType;

import java.io.IOException;
import java.util.function.Supplier;

public class RowIterable implements Supplier<RowBaseIterator>, MycatResponse{
    protected RowBaseIterator rowBaseIterator;

   public static RowIterable create(RowBaseIterator rowBaseIterator){
       return new RowIterable(rowBaseIterator);
   }

    public RowIterable(RowBaseIterator rowBaseIterator) {
        this.rowBaseIterator = rowBaseIterator;
    }
    public RowIterable() {

    }
    @Override
   public MycatResultSetType getType(){
        return MycatResultSetType.RRESULTSET;
    }

    @Override
    public void close() throws IOException {
       if (rowBaseIterator!=null) {
           rowBaseIterator.close();
       }
    }

    @Override
    public RowBaseIterator get() {
        return rowBaseIterator;
    }
}