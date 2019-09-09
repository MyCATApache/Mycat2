package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlParserContext {


    Map<String, Map<String, List<String>>> schemas = new HashMap<>();
    String firstMaybeWrongMessage="";
    String firstToken;
    public void scan() {
        free();
    }



    public void free() {
    }


}