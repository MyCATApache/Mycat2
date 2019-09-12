package io.mycat.sqlparser.util.simpleParser2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class GroupPatternBuilder {
    private final IdRecorder idRecorder;
    private final UTF8Lexer utf8Lexer;
    private final PatternDFG dfg;

    public GroupPatternBuilder() {
        this(0);
    }
    public GroupPatternBuilder(int identifierGenerator) {
        this(identifierGenerator,Collections.emptyMap());
    }

    public GroupPatternBuilder(int identifierGenerator,Map<String, Object> keywords) {
        this.idRecorder = new IdRecorderImpl(true);
        ((IdRecorderImpl) this.idRecorder).load(keywords);
        this.utf8Lexer = new UTF8Lexer(idRecorder);
        this.dfg = new PatternDFG.DFGImpl(identifierGenerator);
    }

    public int addRule(String pattern) {
        return addRule(StandardCharsets.UTF_8.encode(pattern));
    }

    public int addRule(byte[] buffer) {
        return addRule(ByteBuffer.wrap(buffer));
    }

    public int addRule(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        return dfg.addRule(new Iterator<Seq>() {
            @Override
            public boolean hasNext() {
                return utf8Lexer.nextToken();
            }

            @Override
            public Seq next() {
                return idRecorder.createConstToken(null);
            }
        });
    }

    public GroupPattern createGroupPattern() {
        return new GroupPattern(dfg, idRecorder.createCopyRecorder());
    }
}