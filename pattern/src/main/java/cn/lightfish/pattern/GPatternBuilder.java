/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternBuilder {
    private final GPatternIdRecorder idRecorder;
    private final GPatternUTF8Lexer utf8Lexer;
    private final GPatternDFG dfg;

    public GPatternBuilder() {
        this(0);
    }

    public GPatternBuilder(int identifierGenerator) {
        this(identifierGenerator, Collections.emptySet());
    }

    public GPatternBuilder(int identifierGenerator, Set<String> keywords) {
        this.idRecorder = new GPatternIdRecorderImpl(true);
        this.idRecorder.load(keywords);
        this.utf8Lexer = new GPatternUTF8Lexer(idRecorder);
        this.dfg = new GPatternDFG.DFGImpl(identifierGenerator);
    }

    public int addRule(String pattern) {
        return addRule(StandardCharsets.UTF_8.encode(pattern));
    }

    public int addRule(byte[] buffer) {
        return addRule(ByteBuffer.wrap(buffer));
    }

    public int addRule(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        return dfg.addRule(new Iterator<GPatternSeq>() {
            @Override
            public boolean hasNext() {
                return utf8Lexer.nextToken();
            }

            @Override
            public GPatternSeq next() {
                String curTokenString = utf8Lexer.getCurTokenString();
                return idRecorder.createConstToken(curTokenString);
            }
        });
    }
    public GPattern createGroupPattern() {
        return new GPattern(dfg, idRecorder.createCopyRecorder(), GPatternTokenCollector.EMPTY);
    }

    public GPattern createGroupPattern(GPatternTokenCollector collector) {
        return new GPattern(dfg, idRecorder.createCopyRecorder(), collector);
    }

    public GPatternIdRecorder geIdRecorder() {
        return idRecorder;
    }
}