/**
  * Copyright 2017 bejson.com 
  */
package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;
import java.util.List;
import java.util.Map;

/**
 * Created by jamie on 2017/9/10.
 */
public class Schema {

    private List<Matches> matches;
    private String name;
    public void setMatches(List<Matches> matches) {
         this.matches = matches;
     }
     public List<Matches> getMatches() {
         return matches;
     }

    public List<Map<String, Map<String,String>>> blacklist;
    public void setName(String name) {
         this.name = name;
     }
     public String getName() {
         return name;
     }

    public List<Map<String, Map<String,String>>> getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(List<Map<String, Map<String,String>>> blacklist) {
        this.blacklist = blacklist;
    }
}