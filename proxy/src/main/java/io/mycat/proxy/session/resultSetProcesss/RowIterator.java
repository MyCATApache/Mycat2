package io.mycat.proxy.session.resultSetProcesss;

import java.util.Iterator;

public class RowIterator<ROW> implements Iterator <ROW>{
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ROW next() {
        return null;
    }

    public void  close(){

    }
}
