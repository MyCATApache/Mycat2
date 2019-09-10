package io.mycat.sqlparser.util.simpleParser2;

public interface IdRecorder {
    void startRecordTokenChar();

    void recordTokenChar(int c);

    void endRecordTokenChar();
}