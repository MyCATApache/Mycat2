package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

/**
 * Created by jamie on 2017/9/15.
 */
public class DynamicAnnotationManager {
    //  AnnotationSchemaList annotations;
    public static final String annotation_list = "annotations";
    public static final String schema_tag = "schema_tag";
    public static final String schema_name = "name";
    public static final String match_list = "matches";
    public static final String match_tag = "match";
    public static final String match_name = "name";
    public static final String match_state = "state";
    public static final String match_sqltype = "sqltype";
    public static final String match_where = "where";
    public static final String match_tables = "tables";
    public static final String match_actions = "actions";

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param sqlType
     * @return
     */
    public Object get(int sqlType, String tableName) {
        return new Object();
    }






}
