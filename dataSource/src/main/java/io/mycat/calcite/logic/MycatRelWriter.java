package io.mycat.calcite.logic;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.List;

public class MycatRelWriter extends RelWriterImpl {
    public MycatRelWriter(PrintWriter pw) {
        super(pw, SqlExplainLevel.ALL_ATTRIBUTES, true);
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        super.explain_(rel, values);
    }
}