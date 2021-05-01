package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableMultimap;
import lombok.Getter;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;

@Getter
public class MycatViewSqlString {
    private ImmutableMultimap<String, SqlString> sqls;

    public MycatViewSqlString(ImmutableMultimap<String, SqlString> sqls) {
        this.sqls = sqls;
    }
}
