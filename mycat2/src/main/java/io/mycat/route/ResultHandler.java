package io.mycat.route;

import io.mycat.beans.mycat.MycatRowMetaData;

public abstract class ResultHandler {
    ResultHandler output;

    public ResultHandler() {
    }

    abstract  void onColumn(MycatRowMetaData metaData);
    abstract  void onRow(Object[] objects);

    public void setOutput(ResultHandler output){
        this.output = output;
    }

    public abstract void fetch();
}