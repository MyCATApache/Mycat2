/**
 * Copyright 2017 bejson.com
 */
package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;

import java.util.List;
import java.util.Map;

/**
 * Created by jamie on 2017/9/10.
 */
public class Match {

    private List<String> tables;
    private String state;
    private String sqltype;
    private List<Map<String,String>> where;
    private String name;
    private List<Map<String,Map<String,String>>> actions;
    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }



    public List<Map<String, String>> getWhere() {
        return where;
    }

    public void setWhere(List<Map<String, String>> where) {
        this.where = where;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<Map<String, Map<String, String>>> getActions() {
        return actions;
    }

    public void setActions(List<Map<String, Map<String, String>>> actions) {
        this.actions = actions;
    }

    public String getSqltype() {
        return sqltype;
    }

    public void setSqltype(String sqltype) {
        this.sqltype = sqltype;
    }
}