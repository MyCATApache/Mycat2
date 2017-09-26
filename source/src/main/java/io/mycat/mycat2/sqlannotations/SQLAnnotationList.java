package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jamie on 2017/9/23.
 */
public class SQLAnnotationList {
    List<SQLAnnotation> sqlAnnotations;
    public SQLAnnotationList() {
        this.sqlAnnotations = new ArrayList<>();
    }

    public List<SQLAnnotation> getSqlAnnotations() {
        return sqlAnnotations;
    }

    public void setSqlAnnotations(List<SQLAnnotation> sqlAnnotations) {
        this.sqlAnnotations = sqlAnnotations;
    }



    @Override
    public String toString() {
        return "SQLAnnotationList{" +
                "sqlAnnotations=" + sqlAnnotations +
                '}';
    }
}
