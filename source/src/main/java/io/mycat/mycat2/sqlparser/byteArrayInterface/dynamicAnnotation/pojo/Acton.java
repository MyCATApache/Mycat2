package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;

import java.util.Map;

/**
 * Created by jamie on 2017/9/10.
 */
public class Acton {
    String name;
    String classString;
    Map<String,String> parameterList;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassString() {
        return classString;
    }

    public void setClassString(String classString) {
        this.classString = classString;
    }

    public Map<String, String> getParameterList() {
        return parameterList;
    }

    public void setParameterList(Map<String, String> parameterList) {
        this.parameterList = parameterList;
    }
}
