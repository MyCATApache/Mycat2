package io.mycat.sqlparser.util.simpleParser2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IdRecorderImpl implements IdRecorder {
    final static int WORD_LENGTH = 64;
    final byte[] words = new byte[WORD_LENGTH];
    final Map<Integer, Token> longTokenHashMap = new HashMap<>();
    final Map<String, Token> tokenMap = new HashMap<>();
    int offset = 0;
    int hash = 0;
    ///////////position//////////
    int tokenStartOffset;
    int tokenEndOffset;
    final Token tmp = new Token(0, null, null);

    final StringBuilder debugBuffer;

    public IdRecorderImpl(boolean debug) {
        this.debugBuffer = debug ? new StringBuilder() : null;
    }

    public IdRecorder createCopyRecorder() {
        IdRecorderImpl idRecorder = new IdRecorderImpl(false);
        idRecorder.longTokenHashMap.putAll(this.longTokenHashMap);
        idRecorder.tokenMap.putAll(this.tokenMap);
        return idRecorder;
    }

    public void load(Map<String, Object> map) {
        Objects.requireNonNull(map);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            startRecordTokenChar(0);
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            if (key.length > WORD_LENGTH) throw new GroupPatternException.TooLongConstTokenException("{0}",entry.getKey());
            for (byte b : key) {
                append(b);
            }
            endRecordTokenChar(key.length);
            createConstToken(entry.getValue());
        }
    }

    @Override
    public Token getConstToken(String a) {
       return tokenMap.get(a);
    }

    private void addToken(String keyword, Token token) {
        if (keyword.length() > WORD_LENGTH) throw new UnsupportedOperationException();
        if (longTokenHashMap.containsKey(token.hash)) throw new UnsupportedOperationException();
        longTokenHashMap.put(token.hash, token);
        tokenMap.put(keyword, token);
    }

    @Override
    public void append(int b) {
        debugAppend(b);
        words[offset] = (byte) b;
        hash = 31 * hash + b;
        offset = Math.min(++offset, WORD_LENGTH - 1);
    }

    private static boolean equal(long curHash, int length, byte[] word, Token constToken) {
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
            for (int i = 0; i < offset; i++) h = 31 * h + words[i];
            hash = h;
        }
    }

    public boolean isToken(Token token) {
        return equal(hash, offset, words, token);
    }

    ///////////////////////////debug/////////////////////////////////
    private void debugAppend(int c) {
        if (this.debugBuffer != null) this.debugBuffer.append((char) c);
    }

    private void debugClear() {
        if (this.debugBuffer != null) this.debugBuffer.setLength(0);
    }

    public Token createConstToken(Object attr) {
        int length = tokenEndOffset - tokenStartOffset;
        if (length>WORD_LENGTH) throw new GroupPatternException.TooLongConstTokenException("{0}",new String(words));
        Token keyword = longTokenHashMap.get(hash);
        if (keyword != null && equal(hash, length, words, keyword)) {
            return keyword;
        } else if (keyword == null){
            for (int i = 0; i < length; i++) {
                if (0 > words[i]) throw new GroupPatternException.NonASCIICharsetConstTokenException("{0}",new String(words,0,WORD_LENGTH));
            }
            String symbol = new String(this.words, 0, tokenEndOffset - tokenStartOffset).intern();
            Token token = new Token(this.hash, symbol, attr);
            addToken(symbol, token);
            return token;
        }else {
            throw new UnsupportedOperationException();
        }
    }

    public Token toCurToken() {
        hash();
        tmp.startOffset = this.tokenStartOffset;
        tmp.endOffset = this.tokenEndOffset;
        tmp.hash = this.hash;
        Token keyword = longTokenHashMap.get(hash);
        if ((keyword != null) && equal(hash, offset, words, keyword)) {
            tmp.attr = keyword.attr;
            tmp.setSymbol(keyword.getSymbol());
        } else {
            tmp.attr = null;
            tmp.setSymbol(null);
        }
        return tmp;
    }
}