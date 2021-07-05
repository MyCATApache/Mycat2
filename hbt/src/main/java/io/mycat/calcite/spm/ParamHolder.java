package io.mycat.calcite.spm;

import lombok.Data;
import lombok.Getter;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

@Getter
public class ParamHolder {
    public final static ThreadLocal<ParamHolder> CURRENT_THREAD_LOCAL = ThreadLocal.withInitial(() -> new ParamHolder());

    List<Object> params;
    List<SqlTypeName> types;

    public void setData(List<Object> params,
                        List<SqlTypeName> types) {
        this.params = params;
        this.types = types;
    }

    public void clear() {
        params = null;
        types = null;
    }
}
