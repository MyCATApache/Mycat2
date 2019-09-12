package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IdRecorderImpl<T> implements IdRecorder {
    final byte[] word = new byte[8192];
    final Map<Integer, Token<T>> longTokenHashMap = new HashMap<>();
    final Map<String, Token<T>> tokenMap = new HashMap<>();
    int offset = 0;
    int hash = 0;
    ///////////position//////////
    int tokenStartOffset;
    int tokenEndOffset;
    private final Token tmp = new Token(0, null, null);

    final StringBuilder debugBuffer;

    public IdRecorderImpl(boolean debug) {
        this.debugBuffer = debug ? new StringBuilder() : null;
    }

    public void load(Map<String, T> map) {
        Objects.requireNonNull(map);
        map.forEach((key, value) -> addKeyword(key, value));
    }

    private void addKeyword(String keyword, T attr) {
        Token<T> token = new Token<>(getHash(keyword), keyword, attr);
        if (longTokenHashMap.containsKey(token.hash)) {
            throw new UnsupportedOperationException();
        }
        longTokenHashMap.put(token.hash, token);
        tokenMap.put(keyword, token);
    }

    public void append(int b) {
        debugAppend(b);
        word[offset] = (byte) b;
        hash = 31 * hash + b;
        offset++;
    }

    private static boolean equal(long curHash, int length, byte[] word, Token constToken) {
        if (curHash == constToken.hash) {
            String symbol = constToken.symbol;
            for (int i = 0; i < length; i++) {
                if (symbol.charAt(i) != word[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void startRecordTokenChar(int startOffset) {
        this.debugClear();
        this.offset = 0;
        this.hash = 0;
        this.tokenStartOffset = startOffset;
        this.tokenEndOffset = startOffset;
    }

    @Override
    public void recordTokenChar(int c) {
        append(c);
    }

    @Override
    public void endRecordTokenChar(int endOffset) {
        hash();
        this.tokenEndOffset = endOffset;
    }

    private void hash() {
        if (this.hash == 0) {
            int h = 0;
            for (int i = 0; i < offset; i++) {
                h = 31 * h + word[i];
            }
            hash = h;
        }
    }

    public boolean isToken(Token token) {
        return equal(hash, offset, word, token);
    }

    ///////////////////////util/////////////////////////
    private int getHash(String keyword) {
        int length = keyword.length();
        int hash = 0;
        for (int i = 0; i < length; i++) {
            int c = keyword.charAt(i) & 0xff;
            if (0 >= c || c >= 127) {
                throw new UnsupportedOperationException();
            }
            hash = 31 * hash + c;
        }
        return hash;
    }

    ///////////////////////////debug/////////////////////////////////
    private void debugAppend(int c) {
        if (this.debugBuffer != null) this.debugBuffer.append((char) c);
    }

    private void debugClear() {
        if (this.debugBuffer != null) this.debugBuffer.setLength(0);
    }

    public CharSequence getDebugIdString() {
        return (this.debugBuffer != null) ? this.debugBuffer : "";
    }

    public Token toConstToken() {
        Token<T> keyword = longTokenHashMap.get(hash);
        if (keyword != null) {
            return keyword;
        } else {
            try {
                return toCurToken().clone();
            } catch (CloneNotSupportedException e) {
                return null;
            }
        }
    }

    public Token toCurToken() {
        hash();
        tmp.start = this.tokenStartOffset;
        tmp.end = this.tokenEndOffset;
        tmp.hash = this.hash;
        Token<T> keyword = longTokenHashMap.get(hash);
        if ((keyword != null) && equal(hash, offset, word, keyword)) {
            tmp.attr = keyword.attr;
            tmp.symbol = keyword.symbol;
            return tmp;
        } else {
            tmp.attr = null;
            tmp.symbol = null;
            return tmp;
        }
    }
}