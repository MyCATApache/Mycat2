package io.mycat;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;

public class PrototypeService {
   final MetadataManager metadataManager;

    public PrototypeService(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public RowBaseIterator handleSql(SQLStatement statement){
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        return resultSetBuilder.build();
    }
}
