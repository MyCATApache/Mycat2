package io.mycat.prototypeserver.mysql;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class MycatResultSet {
    MycatRowMetaData mycatRowMetaData;
    List<List<Object>> rows;

    @SneakyThrows
    public static MycatResultSet of(List<ResultSet> resultSets) {
        ResultSet resultSet = resultSets.get(0);
        MycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(new JdbcRowMetaData(resultSet.getMetaData()));
        int columnCount = mycatRowMetaData.getColumnCount();
        List<List<Object>> rows = new ArrayList<>();
        for (ResultSet set : resultSets) {
            while (set.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    row.add(set.getObject(i + 1));
                }
                rows.add(row);
            }
        }
        return new MycatResultSet(mycatRowMetaData, rows);
    }

    public RowBaseIterator build() {
        ResultSetBuilder builder = ResultSetBuilder.create();
        for (List<Object> row : rows) {
            builder.addObjectRowPayload(row);
        }
        return builder.build(mycatRowMetaData);
    }

}