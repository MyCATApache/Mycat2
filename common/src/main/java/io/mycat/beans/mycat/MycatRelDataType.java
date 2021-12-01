package io.mycat.beans.mycat;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class MycatRelDataType {
    final List<MycatField> fieldList;

    public MycatRelDataType(List<MycatField> fieldList) {
        this.fieldList = fieldList;
    }

    public static MycatRelDataType of(List<MycatField> mycatFields) {
        return new MycatRelDataType(mycatFields);
    }

    public MycatRelDataType join(MycatRelDataType right) {
        MycatRelDataType left = this;
        ImmutableList.Builder<MycatField> builder = ImmutableList.builder();
        builder.addAll(left.fieldList);
        builder.addAll(right.fieldList);
        return of(builder.build());
    }
}
