package io.mycat.sqlparser.util.simpleParser2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UTF8Lexer {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    int startOffset = 0;
    int limit = 0;
    int position = 0;
    int length = 0;
    public static final byte DEMO = Byte.MIN_VALUE;
    public static final byte END = Byte.MAX_VALUE;
    Map<Long, String> keywords = new HashMap<>();
    List<String> tokens = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    public UTF8Lexer(ByteBuffer buffer, int startOffset, int length) {
        this.buffer = buffer;
        this.startOffset = startOffset;
        this.length = length;
        this.limit = startOffset + length;
    }

    public static void main(String[] args) {
//        String message = "SELECT * FROM `db1`.`mycat_sequence` LIMIT 0, 1000;";
        String message = "/*  9999*/ SELECT (10000.2+2,ssss,\'hahah少时诵诗书\') FROM (db1.`mycat_sequence222`)LIMIT 0, 1000;";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        UTF8Lexer utf8Lexer = new UTF8Lexer(ByteBuffer.wrap(bytes), 0, bytes.length);
        while (utf8Lexer.nextToken()) {


        }
    }

    private boolean isKeyword() {
        return false;
    }


    private int readChar() {
        return Byte.toUnsignedInt(buffer.get(position));
    }

    public void parse() {
        while (nextToken()) {

        }
    }

    public boolean nextToken() {
        skipIgnore();
        if (hasChar()) {
            int c = nextChar();
            if (c == '`') {
                record(c);
                c = nextChar();
                record(c);
                for (; hasChar() && c != '`'; ) {
                    c = nextChar();
                    record(c);
                }
                return true;
            } else if (c == '\'') {
                for (; hasChar(); ) {
                    int peek = peekChar(1);
                    if (c != '\\' && peek == '\'') {
                        position += 2;
                        record(c);
                        return true;
                    }
                    c = peek;
                    ++position;
                }
            }
            {
                boolean id = false;
                while (Character.isLetterOrDigit(c) || c == '_' || c == '$') {
                    record(c);
                    c = nextChar();
                    id = true;
                }
                if (id) {
                    position--;
                    return true;
                }
            }
            record(c);
            return true;
        }
        return false;
    }

    private void skipIgnore() {
        while (hasChar()) {
            byte b = buffer.get(position);
            if (b == ' ') {
                ++position;
                continue;
            } else if (b == '/') {
                if (peekChar(1) == '*') {
                    ++position;
                    for (; hasChar(); ) {
                        if ('*' == peekChar(0) && '/' == peekChar(1)) {
                            position = position + 2;
                            break;
                        }
                        ++position;
                    }
                } else if (peekChar(1) == '/') {
                    skipSingleComment();
                }
            } else if (b == '#') {
                skipSingleComment();
            } else if (b == '-' && peekChar(1) == '-') {
                ++position;
                skipSingleComment();
            } else {
                break;
            }
        }
    }

    private void skipSingleComment() {
        ++position;
        for (; hasChar(); ) {
            if ('\n' == peekChar(1)) {
                ++position;
                break;
            }
            ++position;
        }
    }

    public boolean hasChar() {
        return (position < limit);
    }

    public int peekChar(int step) {
        int ex = position + step;
        if (ex < limit) {
            return Byte.toUnsignedInt(buffer.get(ex));
        } else {
            return END;
        }
    }

    public int nextChar() {
        if (position < limit) {
            int aByte = readChar();
            if (aByte <= 0x007F) {
                position += 1;
                return aByte;
            } else {
                if (aByte <= 0x07FF) {
                    position += 2;
                    return DEMO;
                } else if (aByte <= 0xFFFF) {
                    position += 3;
                    return DEMO;
                } else if (aByte <= 0x1FFFFF) {
                    position += 4;
                    return DEMO;
                } else if (aByte <= 0x3FFFFFF) {
                    position += 5;
                    return DEMO;
                } else {//0x7FFFFFFF
                    position += 6;
                    return DEMO;
                }
            }
        }
        return END;
    }

    private void startRecord() {
        sb.setLength(0);
    }

    private void record(int c) {
        sb.append((byte)c);
    }


}