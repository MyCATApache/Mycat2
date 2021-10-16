package io.mycat.sqlhandler.config;

import java.util.HashMap;
import java.util.Map;

public class Config {
    long version;
    Map<String, Map<String,String>> config = new HashMap<>();
}