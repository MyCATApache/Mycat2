package io.mycat.sqlparser.util.simpleParser2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class GroupPattern {
    private final IdRecorder idRecorder;
    private final UTF8Lexer utf8Lexer;
    private final Matcher matcher;

    public GroupPattern(PatternDFG dfg, IdRecorder copyRecorder) {
        this.idRecorder = copyRecorder;
        this.utf8Lexer = new UTF8Lexer(this.idRecorder);
        this.matcher = dfg.getMatcher();
    }

    public Matcher matcher(String pattern) {
        return matcher(StandardCharsets.UTF_8.encode(pattern));
    }

    public Matcher matcher(byte[] buffer) {
        return matcher(ByteBuffer.wrap(buffer));
    }

    public Matcher matcher(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        matcher.reset();
        while (utf8Lexer.nextToken()) {
            Seq token = idRecorder.toCurToken();
            if (matcher.accept(token)) {
                System.out.println("accept:" + token);
            } else {
                System.out.println("reject:" + token);
            }
        }
        return matcher;
    }

    public Map<String, String> toContextMap(Matcher matcher) {
        Map<String, String> res = new HashMap<>();
        for (Map.Entry<String, Position> entry : matcher.context().entrySet()) {
            Position value = entry.getValue();
            res.put(entry.getKey(), utf8Lexer.getString(value.start, value.end));
        }
        return res;
    }
}