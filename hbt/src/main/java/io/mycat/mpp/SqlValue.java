package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;

//别名
public interface SqlValue extends ASTExp{
    Object getValue(Type type, DataAccessor dataAccessor,DataContext context);

    boolean getValueAsBoolean(Type columns, DataAccessor dataAccessor, DataContext dataContext);
}