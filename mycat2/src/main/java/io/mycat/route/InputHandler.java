package io.mycat.route;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.Getter;

@Getter
public class InputHandler extends ResultHandler {
    private final String targetName;
    private final String sql;



    public InputHandler(String targetName, String sql) {
        this.targetName = targetName;
        this.sql = sql;
    }

    @Override
    public void onColumn(MycatRowMetaData metaData) {

    }

    @Override
    public void onRow(Object[] objects) {

    }

    @Override
    public void fetch() {

    }
}