package io.mycat.vertx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.logical.MycatViewSqlString;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MycatTransientSQLTableScanMappingFunction implements DataNodeMapping, Serializable {
     String targetName;
     String sql;

    @Override
    public MycatViewSqlString apply(List<Object> objects) {
//        SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
        return new MycatViewSqlString(ImmutableMultimap.of(targetName,new SqlString( MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName),sql,ImmutableList.of())) );
    }
//
//    @Override
//    public String toJson() {
//        Map<String,String> map = new HashMap<>();
//        map.put("type",MycatTransientSQLTableScanMappingFunction.class.getName());
//        map.put("targetName",targetName);
//        map.put("sql",sql);
//        return JsonUtil.toJson(map);
//    }
}
