/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPattern {
    private final GPatternIdRecorder idRecorder;
    private final GPatternUTF8Lexer utf8Lexer;
    private final GPatternMatcher matcher;
    private final GPatternTokenCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(GPattern.class);
    private static final boolean DEBUG_ENABLED =
            false;

    public GPattern(GPatternDFG dfg, GPatternIdRecorder copyRecorder, GPatternTokenCollector collector) {
        this.idRecorder = copyRecorder;
        this.collector = collector;
        this.utf8Lexer = new GPatternUTF8Lexer(this.idRecorder);
        this.matcher = dfg.getMatcher(this);
    }

    public GPatternMatcher matcher(String pattern) {
        return matcher(StandardCharsets.UTF_8.encode(pattern));
    }

    public GPatternMatcher matcher(byte[] buffer) {
        return matcher(ByteBuffer.wrap(buffer));
    }

    public GPatternMatcher matcherAndCollect(String pattern) {
        return matcherAndCollect(StandardCharsets.UTF_8.encode(pattern));
    }

    public GPatternMatcher matcher(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        matcher.reset();
        while (utf8Lexer.nextToken()) {
            GPatternSeq token = idRecorder.toCurToken();
            if (matcher.accept(token)) {
                if (DEBUG_ENABLED) LOGGER.debug("accept:{}" + token);
            } else {
                if (DEBUG_ENABLED) LOGGER.debug("reject:{}" + token);
            }
        }
        return matcher;
    }

    public void collect(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        collector.onCollectStart();
        while (utf8Lexer.nextToken()) {
            GPatternSeq token = idRecorder.toCurToken();
            collector.collect(token);
        }
        collector.onCollectEnd();
    }

    public GPatternMatcher matcherAndCollect(ByteBuffer buffer) {
        utf8Lexer.init(buffer, 0, buffer.limit());
        matcher.reset();
        collector.onCollectStart();
        while (utf8Lexer.nextToken()) {
            GPatternSeq token = idRecorder.toCurToken();
            if (matcher.accept(token)) {
                if (DEBUG_ENABLED) LOGGER.debug("accept:{}" + token);
            } else {
                if (DEBUG_ENABLED) LOGGER.debug("reject:{}" + token);
            }
            collector.collect(token);
        }
        collector.onCollectEnd();
        return matcher;
    }

    public Map<String, String> toContextMap(GPatternMatcher matcher) {
        Map<String, String> res = new HashMap<>();
        for (Map.Entry<String, GPatternPosition> entry : matcher.positionContext().entrySet()) {
            GPatternPosition value = entry.getValue();
            if (value.end < 0) {
                continue;
            }
            res.put(entry.getKey(), utf8Lexer.getString(value.start, value.end));
        }
        return res;
    }

    public GPatternIdRecorder getIdRecorder() {
        return idRecorder;
    }

    public GPatternUTF8Lexer getUtf8Lexer() {
        return utf8Lexer;
    }

    public GPatternMatcher getMatcher() {
        return matcher;
    }

    public <T> T getCollector() {
        return (T) collector;
    }
}