/**
  * Copyright 2017 bejson.com 
  */
package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo;
import java.util.List;

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

    public void setName(String name) {
         this.name = name;
     }
     public String getName() {
         return name;
     }

}