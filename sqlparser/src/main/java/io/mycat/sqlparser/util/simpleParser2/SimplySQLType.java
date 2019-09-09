package io.mycat.sqlparser.util.simpleParser2;

import lombok.Data;

import java.util.List;

@Data
public class SimplySQLType {
    long[] tables ;
    String firstMaybeWrongMessage;
    long firstToken;
}