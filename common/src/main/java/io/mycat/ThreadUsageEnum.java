package io.mycat;
/**
 * @author Junwen Chen
 **/
public enum ThreadUsageEnum {
    THIS_THREADING,
    BINDING_THREADING,
    MULTI_THREADING
    ,
    ;

    public static final ThreadUsageEnum DEFAULT = ThreadUsageEnum.THIS_THREADING;

}