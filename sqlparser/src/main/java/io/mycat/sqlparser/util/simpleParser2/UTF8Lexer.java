package io.mycat.sqlparser.util.simpleParser2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UTF8Lexer {
    ByteBuffer buffer;
    int limit = 0;
    int position = 0;
    private final IdRecorder idRecorder;
    public static final byte DEMO = Byte.MIN_VALUE;
    public static final byte END = Byte.MAX_VALUE;

    public UTF8Lexer(IdRecorder idRecorder) {
        this.idRecorder = idRecorder;
    }

    public void init(String text) {
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(text);
        init(byteBuffer, 0, byteBuffer.limit());
    }

    public void init(ByteBuffer buffer, int startOffset, int length) {
        this.buffer = buffer;
        this.position = startOffset;
        this.limit = startOffset + length;
        this.idRecorder.startRecordTokenChar(position);
    }

    public boolean nextToken() {
        skipIgnore();
        if (!hasChar()) return false;
        idRecorder.startRecordTokenChar(position);
        int c = nextChar();
        char cc = (char) c;
        if (c == '`' || c == '\'') {
            idRecorder.append(c);
            pickTo(c);
            return true;
        }
        boolean id = false;
        while (hasChar() && (Character.isLetterOrDigit(c) || c == '_' || c == '$')) {
            idRecorder.append(c);
            c = nextChar();
            id = true;
        }
        if (id) {
            --position;
            idRecorder.endRecordTokenChar(position);
            return true;
        } else {
            idRecorder.append(c);
            idRecorder.endRecordTokenChar(position);
            return true;
        }
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

    private void skipIgnore() {
        while (hasChar()) {
            byte b = buffer.get(position);
            if (b == ' ') {
                ++position;
                continue;
            } else if (b == '/') {
                if (peekChar(1) == '*') {
                    position += 2;
                    for (; hasChar(); ) {
                        if ('*' == peekChar(0) && '/' == peekChar(1)) {
                            position += 2;
                            break;
                        }
                        ++position;
                    }
                    continue;
                } else if (peekChar(1) == '/') {
                    position += 2;
                    skipSingleComment();
                    continue;
                } else {
                    break;
                }
            } else if (b == '#') {
                position += 1;
                skipSingleComment();
                continue;
            } else if (b == '-' && peekChar(1) == '-') {
                position += 2;
                skipSingleComment();
                continue;
            } else {
                break;
            }
        }
    }

    private void skipSingleComment() {
        for (; hasChar(); ) {
            if ('\n' == peekChar(1)) {
                position += 2;
                break;
            }
            ++position;
        }
    }

    public boolean hasChar() {
        return position < limit;
    }

    public int peekChar(int step) {
        int ex = position + step;
        return (ex < limit) ? Byte.toUnsignedInt(buffer.get(ex)) : END;
    }

    public int nextChar() {
        if (!hasChar()) return END;
        int aByte = Byte.toUnsignedInt(buffer.get(position));
        if (aByte <= 0x007F) {
            position += 1;
            return aByte;
        } else {
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
        }
    }

    public String getCurTokenString() {
        Token token = idRecorder.toCurToken();
        return getString(token.getStartOffset(), token.getEndOffset());
    }

    public String getString(int start, int end) {
        byte[] bytes = new byte[end - start];
        for (int i = 0; start < end; start++, i++) {
            bytes[i] = buffer.get(start);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}