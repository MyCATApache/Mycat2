package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class PatternRootConfig {
    private UserConfig user;
    private List<Map<String,Object>> sqls = new ArrayList<>();
    private List<List<Map<String,Object>>> sqlsGroup = new ArrayList<>();
    private Map<String,Object> defaultHanlder;
    private String transactionType;
    private String matcherClazz;

    public List<Map<String,Object>> getSqls() {//注意去重
        return Stream.concat(sqlsGroup.stream().flatMap(i -> i.stream()), sqls.stream()).distinct().collect(Collectors.toList());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserConfig {
        private String username;
        private String password;
        private String ip;
    }

//    @Data
//    @ToString
//    public static class TextItemConfig {
//        String name;
//        String sql;
//        //handler
//        List<String> hints = new ArrayList<>();
//        Map<String, String> tags = new HashMap<>();
//        String command;
//        String explain;
//        String cache;
//    }

    public static void main(String[] args) {
    }
}