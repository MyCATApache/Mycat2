package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.util.MycatRowMetaDataImpl;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;

@EqualsAndHashCode
@Getter
public  class SQLKey {
    RelNode mycatView;
    String targetName;
    SqlString sql;

    public SQLKey(RelNode mycatView, String targetName, SqlString value) {
        this.mycatView = mycatView;
        this.targetName = targetName;
        this.sql = value;
    }
     public MycatRowMetaData getRowMetaData(){
        return new CalciteRowMetaData(mycatView.getRowType().getFieldList());
    }
}