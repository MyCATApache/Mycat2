package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

//别名
public interface SqlValue extends ASTExp {
    default  Object getValue(RowType type, DataAccessor dataAccessor, DataContext context){
        throw new UnsupportedOperationException();
    }

    default boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        throw new UnsupportedOperationException();
    }

//    default int getValueAsInt(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
//        throw new UnsupportedOperationException();
//    }
//    default int getValueAsInt(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
//        throw new UnsupportedOperationException();
//    }
    Type getType();


}