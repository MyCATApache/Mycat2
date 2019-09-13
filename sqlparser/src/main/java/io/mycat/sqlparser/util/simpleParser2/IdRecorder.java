package io.mycat.sqlparser.util.simpleParser2;

import java.util.Map;

public interface IdRecorder {
    void startRecordTokenChar(int startOffset);

    void append(int c);

    void endRecordTokenChar(int endOffset);

    Token createConstToken(Object o);

    Token toCurToken();

    IdRecorder createCopyRecorder();

    public void load(Map<String, Object> map);

    Token getConstToken(String a);
}