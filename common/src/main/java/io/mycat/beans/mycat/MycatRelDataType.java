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

    public MycatRelDataType rename(List<String> names){
        if (fieldList.size() != names.size()){
            throw new IllegalArgumentException();
        }
        ImmutableList.Builder<MycatField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldList.size(); i++) {
            MycatField mycatField = fieldList.get(i);
            String newName = names.get(i);
            if (newName.equals(mycatField.getName())){
                builder.add(mycatField);
            }else {
                builder.add(mycatField.rename(newName));
            }
        }
       return MycatRelDataType.of(builder.build());
    }
}
