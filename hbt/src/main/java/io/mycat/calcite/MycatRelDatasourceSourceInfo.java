package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logical.MycatView;
import io.mycat.util.JsonUtil;
import io.mycat.vertx.DataNodeMapping;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
public class MycatRelDatasourceSourceInfo implements Serializable {
    private final MycatRowMetaData columnInfo;
    private final SqlNode sqlTemplate;
    public int refCount = 0;
    private final MycatView view;

    public MycatRelDatasourceSourceInfo(MycatRowMetaData columnInfo, SqlNode sqlTemplate,MycatView view) {
        this.columnInfo = columnInfo;
        this.sqlTemplate = sqlTemplate;
        this.view = view;
    }
}