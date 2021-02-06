package io.mycat.calcite;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.MycatDataContext;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.util.SqlString;

import java.util.*;

@Getter
public class CodeExecuterContext {
    private IdentityHashMap<RelNode,Integer> mycatViews;
    final Map<String, Object> context;
    final ArrayBindable bindable;
    final String code;
    final boolean forUpdate;


    public CodeExecuterContext(IdentityHashMap<RelNode,Integer> mycatViews, Map<String, Object> context, ArrayBindable bindable, String code, boolean forUpdate) {
        this.mycatViews = mycatViews;
        this.context = context;
        this.bindable = bindable;
        this.code = code;
        this.forUpdate = forUpdate;
    }

    public static final CodeExecuterContext of(IdentityHashMap<RelNode,Integer> mycatViews, Map<String, Object> context,
                                               ArrayBindable bindable,
                                               String code, boolean forUpdate) {
        return new CodeExecuterContext(mycatViews, context, bindable, code, forUpdate);
    }

}