package io.mycat.sqlparser.util.simpleParser2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IdRecorderImpl<T> implements IdRecorder {
    final byte[] word = new byte[8192];
    final Map<Integer, TokenImpl<T>> longTokenHashMap = new HashMap<>();
    final Map<String, TokenImpl<T>> tokenMap = new HashMap<>();
    int offset = 0;
    int hash = 0;
    ///////////position//////////
    int tokenStartOffset;
    int tokenEndOffset;
    final TokenImpl tmp = new TokenImpl(0, null, null);

    final StringBuilder debugBuffer;

    public IdRecorderImpl(boolean debug) {
        this.debugBuffer = debug ? new StringBuilder() : null;
    }

    public void load(Map<String, T> map) {
        Objects.requireNonNull(map);
        for (Map.Entry<String, T> entry : map.entrySet()) {
            startRecordTokenChar(0);
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            for (byte b : key) {
                append(b);
            }
            startRecordTokenChar(key.length);
            createConstToken(entry.getValue());
        }
    }

    private void addToken(String keyword, TokenImpl<T> token) {
        if (longTokenHashMap.containsKey(token.hash)) {
            throw new UnsupportedOperationException();
        }
        longTokenHashMap.put(token.hash, token);
        tokenMap.put(keyword, token);
    }
    @Override
    public void append(int b) {
        debugAppend(b);
        word[offset] = (byte) b;
        hash = 31 * hash + b;
        offset++;
    }

    private static boolean equal(long curHash, int length, byte[] word, TokenImpl constToken) {
        if (curHash == constToken.hash) {
            String symbol = constToken.getSymbol();
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

    public boolean isToken(TokenImpl token) {
        return equal(hash, offset, word, token);
    }

    ///////////////////////////debug/////////////////////////////////
    private void debugAppend(int c) {
        if (this.debugBuffer != null) this.debugBuffer.append((char) c);
    }

    private void debugClear() {
        if (this.debugBuffer != null) this.debugBuffer.setLength(0);
    }

    public TokenImpl createConstToken(T attr) {
        TokenImpl<T> keyword = longTokenHashMap.get(hash);
        if (keyword != null) {
            return keyword;
        } else {
            for (int i = tokenStartOffset; i < tokenEndOffset; i++) {
                byte b = word[i];
                if (0 > b) {
                    throw new UnsupportedOperationException();
                }
            }
            String symbol = new String(this.word, 0, tokenEndOffset - tokenStartOffset);
            TokenImpl<T> token = new TokenImpl<>(this.hash, symbol, attr);
            addToken(symbol, token);
            return token;
        }
    }

    public TokenImpl toCurToken() {
        hash();
        tmp.startOffset = this.tokenStartOffset;
        tmp.endOffset = this.tokenEndOffset;
        tmp.hash = this.hash;
        TokenImpl<T> keyword = longTokenHashMap.get(hash);
        if ((keyword != null) && equal(hash, offset, word, keyword)) {
            tmp.attr = keyword.attr;
            tmp.setSymbol(keyword.getSymbol());
            return tmp;
        } else {
            tmp.attr = null;
            tmp.setSymbol(null);
            return tmp;
        }
    }
}