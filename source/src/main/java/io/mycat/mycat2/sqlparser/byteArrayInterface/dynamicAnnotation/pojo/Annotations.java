/**
  * Copyright 2017 bejson.com 
  */
package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;

import java.util.List;
import java.util.Map;

/**
 * Created by jamie on 2017/9/10.
 */
public class Annotations {
    private List<Map<String, Map<String,String>>> global;
    private Schema schema;
    public void setSchema(Schema schema) {
         this.schema = schema;
     }
     public Schema getSchema() {
         return schema;
     }

    public List<Map<String, Map<String,String>>> getGlobal() {
        return global;
    }

    public void setGlobal(List<Map<String, Map<String,String>>> global) {
        this.global = global;
    }
}