package io.mycat.pattern;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public final class GPatternIdRecorderImpl implements GPatternIdRecorder {
    final static int WORD_LENGTH = 64;
    final HashMap<Integer,GPatternToken> longTokenHashMap = new HashMap<>();
    final Map<String, GPatternToken> tokenMap = new HashMap<>();
    public final static int BASIC = 0x811c9dc5;
    public final static int PRIME = 0x01000193;
    final GPatternToken tmp = new GPatternToken(0, 0, null, null);
    private GPatternUTF8Lexer lexer;
    private final boolean ignoreCase;
    private boolean debug;

    public GPatternIdRecorderImpl(boolean debug) {
        this(debug, true);
    }

    public GPatternIdRecorderImpl(boolean debug, boolean ignoreCase) {
        this.debug = debug;
        this.ignoreCase = ignoreCase;
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

    private int fnv1a_32(byte[] input) {
        if (input == null) {
            return 0;
        }
        long hash = BASIC;
        for (byte c : input) {
            int cc = c & 0xff;
            if (ignoreCase) {
                hash ^= a2A[cc];
            } else {
                hash ^= cc;
            }
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
        b = b & 0xff;
        if (ignoreCase) {
            hash ^= a2A[b];
        } else {
            hash ^= b;
        }
        hash *= PRIME;
        tmp.hash = hash;
    }

    static final byte[] a2A = new byte[256];

    static {
        IntStream.range(0, 256).forEach(i -> a2A[i] = (byte) i);
        IntStream.range(65, 90).forEach(i -> a2A[i] = (byte) (i + ('a' - 'A')));
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