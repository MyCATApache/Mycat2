package cn.lightfish.pattern;

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public final class GPatternIdRecorderImpl implements GPatternIdRecorder {
    final static int WORD_LENGTH = 64;
    final IntObjectHashMap<GPatternToken> longTokenHashMap = IntObjectHashMap.newMap();
    final Map<String, GPatternToken> tokenMap = new HashMap<>();
    public final static int BASIC = 0x811c9dc5;
    public final static int PRIME = 0x01000193;
    final GPatternToken tmp = new GPatternToken(0, 0, null, null);
    private GPatternUTF8Lexer lexer;

    public GPatternIdRecorderImpl(boolean debug) {

    }

    public GPatternIdRecorder createCopyRecorder() {
        GPatternIdRecorderImpl idRecorder = new GPatternIdRecorderImpl(false);
        idRecorder.longTokenHashMap.putAll(this.longTokenHashMap);
        idRecorder.tokenMap.putAll(this.tokenMap);
        return idRecorder;
    }

    public void load(Set<String> map) {
        Objects.requireNonNull(map);
        for (String entry : map) {
            startRecordTokenChar(0);
            byte[] key = entry.getBytes(StandardCharsets.UTF_8);
            if (key.length > WORD_LENGTH)
                throw new GPatternException.TooLongConstTokenException("{0}", entry);
            for (byte b : key) {
                append(b);
            }
            endRecordTokenChar(key.length);
            createConstToken(entry);
        }
    }

    @Override
    public GPatternToken getConstToken(String a) {
        return tokenMap.get(a);
    }

    @Override
    public void setLexer(GPatternUTF8Lexer lexer) {
        this.lexer = lexer;
        this.tmp.lexer = lexer;
    }


    private void addToken(String keyword, GPatternToken token) {
        if (keyword.length() > WORD_LENGTH) throw new UnsupportedOperationException();
        if (longTokenHashMap.containsKey(token.hash)) {
            throw new UnsupportedOperationException();
        }
        longTokenHashMap.put(token.hash, token);
        tokenMap.put(keyword, token);
    }

    private static int fnv1a_32(byte[] input) {
        if (input == null) {
            return 0;
        }
        long hash = BASIC;
        for (byte c : input) {
            hash ^= c & 0xff;
            hash *= PRIME;
        }
        return (int) hash;
    }

    @Override
    public void startRecordTokenChar(int startOffset) {
        tmp.hash = BASIC;
        tmp.startOffset = startOffset;
    }

    @Override
    public void endRecordTokenChar(int endOffset) {
        tmp.endOffset = endOffset;
        tmp.length = endOffset - tmp.startOffset;
    }

//    @Override
//    public void rangeRecordTokenChar(int startOfffset, int endOffset) {
//        ByteBuffer buffer = lexer.buffer;
//        startRecordTokenChar(startOfffset);
//        for (int i = startOfffset; i <endOffset ; i++) {
//            append(buffer.get(i));
//        }
//        endRecordTokenChar(endOffset);
//    }

    @Override
    public final void append(int b) {
        int hash = tmp.hash;
        hash ^= b & 0xff;
        hash *= PRIME;
        tmp.hash = hash;
    }

    public GPatternToken createConstToken(String keywordText) {
        byte[] words = keywordText.getBytes(StandardCharsets.UTF_8);
        int hash = fnv1a_32(words);
        if (words.length > WORD_LENGTH) throw new GPatternException.TooLongConstTokenException("{0}", keywordText);
        GPatternToken keyword = longTokenHashMap.get(hash);
        if (keyword != null) {
            return keyword;
        } else {
            for (byte word : words) {
                if (0 > word) throw new GPatternException.NonASCIICharsetConstTokenException("{0}", keywordText);
            }
            GPatternToken token = new GPatternToken(hash, words.length, keywordText, lexer);
            addToken(keywordText, token);
            return token;
        }
    }

    public GPatternToken toCurToken() {
        return tmp;
    }
}