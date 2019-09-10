package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IdRecorderImpl<T> implements IdRecorder {
    final byte[] word = new byte[8192];
    final Map<Long, Token<T>> longTokenHashMap = new HashMap<>();
    final Map<String, Token<T>> tokenMap = new HashMap<>();
    int offset = 0;
    long hash = 0;
    Token currentAttr;

    final StringBuilder debugBuffer;

    public IdRecorderImpl(boolean debug) {
        this.debugBuffer = debug ? new StringBuilder() : null;
    }

    public void load(Map<String, T> map) {
        Objects.requireNonNull(map);
        for (Map.Entry<String, T> entry : map.entrySet()) {
            String keyword = entry.getKey();
            long hash = getHash(keyword);
            Token<T> token = new Token<>(hash, keyword, entry.getValue());
            if (longTokenHashMap.containsKey(hash)) {
                throw new UnsupportedOperationException();
            }
            longTokenHashMap.put(hash, token);
            tokenMap.put(keyword, token);
        }
    }

    public void append(int b) {
        debugAppend(b);
        if (offset < 9) {
            hash = hash << 8 | b;
        } else {
            word[offset] = (byte) b;
        }
        ++offset;
    }

    public boolean isSymbol() {
        return (toCurToken() != null);
    }

    private static boolean equal(long curHash, int length, byte[] word, Token attr) {
        if (curHash == attr.hash) {
            String symbol = attr.getSymbol();
            for (int i = 8; i < length; i++) {
                if (symbol.charAt(i) != word[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Token<T> getToken(String id) {
        return tokenMap.get(id);
    }

    public T getCurrentAttr() {
        return (currentAttr != null) ? (T) currentAttr.getAttr() : null;
    }

    @Override
    public void startRecordTokenChar() {
        debugClear();
        this.endRecordTokenChar();
    }

    @Override
    public void recordTokenChar(int c) {
        append(c);
    }

    @Override
    public void endRecordTokenChar() {
        offset = 0;
        hash = 0;
        currentAttr = null;
    }

    public boolean isToken(Token token) {
        return equal(hash, offset, word, token);
    }

    ///////////////////////util/////////////////////////
    private static long getHash(String keyword) {
        long hash = 0;
        int length = Math.min(8, keyword.length());
        for (int i = 0; i < length; i++) {
            char c = keyword.charAt(i);
            if (Character.isIdentifierIgnorable(c)) {
                hash = hash << 8 | (byte) c;
            } else {
                throw new UnsupportedOperationException();
            }
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

    public Token toCurToken() {
        Token<T> attr = longTokenHashMap.get(hash);
        if ((attr != null) && equal(hash, offset, word, attr)) {
            return attr;
        } else {
            return null;
        }
    }
}