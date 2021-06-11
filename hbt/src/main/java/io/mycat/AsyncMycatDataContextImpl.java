package io.mycat;

import hu.akarnokd.rxjava3.parallel.ParallelTransformers;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.RxBuiltInMethodImpl;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AsyncMycatDataContextImpl extends NewMycatDataContextImpl {

    public AsyncMycatDataContextImpl(MycatDataContext dataContext,
                                     CodeExecuterContext context,
                                     List<Object> params,
                                     boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
    }
}
