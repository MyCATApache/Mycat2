package io.mycat.beans.mycat;

import lombok.Getter;

@Getter
public class MycatField {
    private final String name;
    private final MycatDataType mycatDataType;
    private final byte scale;
    private final byte precision;
    private final boolean nullable;

    public MycatField(String name,MycatDataType mycatDataType,boolean nullable) {
        this.name = name;
        this.mycatDataType = mycatDataType;
        this.scale = 0;
        this.precision = 0;
        this.nullable = nullable;
    }
    public MycatField(String name,MycatDataType mycatDataType,boolean nullable,int scale, int precision) {
        this.name = name;
        this.mycatDataType = mycatDataType;
        this.scale = (byte) scale;
        this.precision = (byte) precision;
        this.nullable = nullable;
    }
    public static MycatField of(String name,MycatDataType mycatDataType,boolean nullable){
        return new MycatField(name,mycatDataType,nullable,0,0);
    }
    public static MycatField of(String name,MycatDataType mycatDataType,boolean nullable,int scale,int precision){
        return new MycatField(name,mycatDataType,nullable,scale,precision);
    }
    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }
}
