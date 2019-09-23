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


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternUTF8Lexer {
    ByteBuffer buffer;
    int limit = 0;
    int position = 0;
    private final GPatternIdRecorder idRecorder;
    public static final int DEMO = 128;

    public GPatternUTF8Lexer(GPatternIdRecorder idRecorder) {
        this.idRecorder = idRecorder;
        this.idRecorder.setLexer(this);
    }

    public void init(String text) {
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(text);
        init(byteBuffer, 0, byteBuffer.limit());
    }

    public void init(ByteBuffer buffer, int startOffset, int limit) {
        this.buffer = buffer;
        this.position = startOffset;
        this.limit = limit;
    }

    public boolean nextToken() {
        for (; position < limit; ) {
            int startPos = position;
            int b;
            byte handler = HANDLER[b = nextChar()];
            char c = (char) b;
            switch (handler) {
                case ID_HANDLER: {
                    return idHanlder(startPos, b);
                }
                case BLANK_SPACE_HANDLER: {
                    continue;
                }
                case KEYWORD_HANDLER:
                case STRING_HANLDER: {
                    return stringHandler(startPos, b);
                }
                case LEFT_SLASH_HANDLER: {
                    if (leftSlashHanlder()) continue;
                    return singleChar(startPos, b);
                }
                case DASH_HANDLER: {
                    if (dashHandler()) continue;
                    // return singleChar(startPos, b);
                }
                default:
                    return singleChar(startPos, b);
            }
        }
        return false;
    }

    private boolean leftSlashHanlder() {
        int c = peekAsciiChar(0);
        if (c == '*') {
            skipMultiComment();
            return true;
        } else if (c == '/') {
            skipSingleComment();
            return true;
        }
        return false;
    }

    private boolean idHanlder(int startPos, int b) {
        idRecorder.startRecordTokenChar(startPos);
        idRecorder.append(b);
        while (hasChar()) {
            int backup = position;
            b = nextChar();
            if (isIdChar(b)) {
                idRecorder.append(b);
            } else {
                position = backup;
                break;
            }
        }
        idRecorder.endRecordTokenChar(position);
        return true;
    }

    private boolean stringHandler(int startPos, int b) {
        idRecorder.startRecordTokenChar(startPos);
        idRecorder.append(b);
        pickTo(b);
        return true;
    }

    private boolean dashHandler() {
        if (peekAsciiChar(0) == '-') {
            skipSingleComment();
            return true;
        }
        return false;
    }

    private void skipMultiComment() {
        position += 1;
        for (; hasChar(); ) {
            if ('*' == peekAsciiChar(0) && '/' == peekAsciiChar(1)) {
                position += 2;
                break;
            }
            ++position;
        }
    }

    private boolean singleChar(int startPos, int b) {
        char c = (char) b;
        idRecorder.startRecordTokenChar(startPos);
        idRecorder.append(b);
        idRecorder.endRecordTokenChar(position);
        return true;
    }

    final static byte[] HANDLER = new byte[129];
    final static int KEYWORD_HANDLER = 7;
    final static int ID_HANDLER = 1;
    final static int BLANK_SPACE_HANDLER = 2;
    final static int STRING_HANLDER = 3;
    final static int LEFT_SLASH_HANDLER = 4;
    final static int DASH_HANDLER = 5;
    final static int SHARP_HANLDER = 6;

    static {
        for (int i = '0'; i <= '9'; i++) {
            HANDLER[i] = ID_HANDLER;
        }
        for (int i = 'A'; i <= 'Z'; i++) {
            HANDLER[i] = ID_HANDLER;
        }
        for (int c = 'a'; c <= 'z'; c++) {
            HANDLER[c] = ID_HANDLER;
        }
        HANDLER['`'] = KEYWORD_HANDLER;
        HANDLER['_'] = ID_HANDLER;
        HANDLER['$'] = ID_HANDLER;
        HANDLER[DEMO] = ID_HANDLER;

        HANDLER[' '] = BLANK_SPACE_HANDLER;
        HANDLER['#'] = SHARP_HANLDER;
        HANDLER['\''] = STRING_HANLDER;
        HANDLER['\"'] = STRING_HANLDER;
        HANDLER['/'] = LEFT_SLASH_HANDLER;
        HANDLER['-'] = DASH_HANDLER;
    }

    private boolean isIdChar(int c) {
        return HANDLER[c] == ID_HANDLER;
    }

    private void pickTo(final int t) {
        int c = t;
        do {
            int peek = nextChar();
            idRecorder.append(peek);
            if (c != '\\' && peek == t) {
                idRecorder.endRecordTokenChar(position);
                break;
            }
            c = peek;
        } while (hasChar());
    }

    private void skipSingleComment() {
        position += 1;
        for (; hasChar(); ) {
            if ('\n' == peekAsciiChar(0)) {
                ++position;
                break;
            }
            ++position;
        }
    }

    public boolean hasChar() {
        return position < limit;
    }

    public int peekAsciiChar(int step) {
        int ex = position + step;
        return buffer.get(ex);
    }

    public int nextChar() {
        int aByte = buffer.get(position);
        if (aByte < 0) {//0x007F
            aByte = Byte.toUnsignedInt((byte) aByte);
            if (aByte <= 0x07FF) {
                position += 3;
            } else if (aByte <= 0xFFFF) {
                position += 4;
            } else if (aByte <= 0x1FFFFF) {
                position += 5;
            } else if (aByte <= 0x3FFFFFF) {
                position += 6;
            } else {//0x7FFFFFFF
                position += 7;
            }
            return DEMO;
        } else {
            position += 1;
            return aByte;
        }
    }

    public String getCurTokenString() {
        GPatternToken token = idRecorder.toCurToken();
        return getString(token.getStartOffset(), token.getEndOffset());
    }

    public String getString(int start, int end) {
        byte[] bytes = new byte[end - start];
        for (int i = 0; start < end; start++, i++) {
            bytes[i] = buffer.get(start);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public boolean fastEquals(int startOffset, int endOffset, byte[] symbol) {
        if (buffer.hasArray()) {
            byte[] array = buffer.array();
            return arrayEquals(startOffset, endOffset, symbol, array);
        } else {
            return directEquals(startOffset, endOffset, symbol);
        }

    }

    private boolean directEquals(int startOffset, int endOffset, byte[] symbol) {
        int length = symbol.length;
        for (int j = 4; j < length; j++) {
            if (buffer.get(startOffset + j) != symbol[j]) {
                return false;
            }
        }
        return true;
    }

    private boolean arrayEquals(int startOffset, int endOffset, byte[] symbol, byte[] array) {
        int length = symbol.length;
        for (int j = 4; j < length; j++) {
            if (array[startOffset + j] != symbol[j]) {
                return false;
            }
        }
        return true;
    }
}