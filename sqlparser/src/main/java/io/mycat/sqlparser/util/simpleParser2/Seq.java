package io.mycat.sqlparser.util.simpleParser2;

public interface Seq {
        String getSymbol();

        int getStartOffset();

        int getEndOffset();
    }