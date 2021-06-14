package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logical.MycatView;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import java.io.Serializable;

@Getter
public class MycatRelDatasourceSourceInfo implements Serializable {
    private final MycatRowMetaData columnInfo;
    private final SqlNode sqlTemplate;
    public int refCount = 0;
    private final RelNode relNode;

    public MycatRelDatasourceSourceInfo(MycatRowMetaData columnInfo, SqlNode sqlTemplate,RelNode relNode) {
        this.columnInfo = columnInfo;
        this.sqlTemplate = sqlTemplate;
        this.relNode = relNode;
    }

    public <T extends RelNode> T  getRelNode() {
        return (T)relNode;
    }
}