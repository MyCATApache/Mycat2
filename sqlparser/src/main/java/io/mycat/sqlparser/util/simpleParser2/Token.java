package io.mycat.sqlparser.util.simpleParser2;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Token<T> {
    final long hash;
    final String symbol;
    final T attr;
}