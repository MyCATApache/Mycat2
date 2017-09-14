/**
 * Copyright 2017 bejson.com
 */
package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;

import java.util.List;

/**
 * Created by jamie on 2017/9/10.
 */
public class RootBean {

    private List<Annotations> annotations;

    public void setAnnotations(List<Annotations> annotations) {
        this.annotations = annotations;
    }

    public List<Annotations> getAnnotations() {
        return annotations;
    }
}