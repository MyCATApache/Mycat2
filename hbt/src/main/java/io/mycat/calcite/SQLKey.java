package io.mycat.calcite;

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
}