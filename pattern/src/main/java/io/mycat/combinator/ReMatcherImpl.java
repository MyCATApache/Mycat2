package io.mycat.combinator;

import java.nio.CharBuffer;


/**
 * https://github.com/xt0fer/RegExpGuts
 */
public final class ReMatcherImpl {

    private final char[] regex;
    private CharBuffer text;

    public ReMatcherImpl(String text) {
        this.regex = text.toCharArray();
    }

    //    /* match: search for regexp anywhere in text */
//    int match(char *regexp, char *text)
//    {
//        if (regexp[0] == '^')
//            return matchhere(regexp+1, text);
//        do {    /* must look even if string is empty */
//            if (matchhere(regexp, text))
//                return 1;
//        } while (*text++ != '\0');
//        return 0;
//    }
    public boolean match(CharBuffer text) {
        try {
            this.text = text;
            int r = 0;
            int t = 0;

            if (this.regex[r] == '^') {
                return (matchhere(r + 1, t) == 1);
            }
            do {
                if (matchhere(r, t) == 1) {
                    return true;
                }
            } while (t++ != text.length());
            return false;
        } finally {
            this.text = null;
        }
    }

    //    /* matchhere: search for regexp at beginning of text */
//    int matchhere(char *regexp, char *text)
//    {
//        if (regexp[0] == '\0') return 1;
//        if (regexp[1] == '*')
//            return matchstar(regexp[0], regexp+2, text);
//        if (regexp[0] == '$' && regexp[1] == '\0') return *text == '\0';
//        if (*text!='\0' && (regexp[0]=='.' || regexp[0]==*text)) return matchhere(regexp+1, text+1);
//        return 0;
//    }
    final private int matchhere(int r, int t) {
        if (r == this.regex.length) return 1;
        if ((r + 1 < this.regex.length) && (this.regex[r + 1] == '*'))
            return matchstar(this.regex[r], r + 2, t);
        if ((r + 1 < this.regex.length) && (this.regex[r + 1] == '+'))
            return matchplus(this.regex[r], r + 2, t);
        if (this.regex[r] == '$' && ((r + 1) == this.regex.length))
            return (t == text.limit()) ? 1 : 0;
        if ((t != this.text.limit()) && (this.regex[r] == '.' || this.regex[r] == this.text.charAt(t)))
            return matchhere(r + 1, t + 1);
        return 0;
    }

    //    /* matchstar: search for c*regexp at beginning of text */
//    int matchstar(int c, char *regexp, char *text)
//    {
//        do {    /* a * matches zero or more instances */
//            if (matchhere(regexp, text))
//                return 1;
//        } while (*text != '\0' && (*text++ == c || c == '.'));
//
//        return 0;
//    }
    final private int matchstar(char c, int r, int t) {
        do {
            if (matchhere(r, t) == 1) {
                return 1;
            }
        } while ((t != this.text.limit()) && ((this.text.charAt(t++) == c) || (c == '.')));
        return 0;
    }

    //    int
//    matchplus(int c, char *regexp, char *text)
//    {
//        int i = 0;
//        do { //a matches one or more instances
//            if (matchhere(regexp, text))
//                if (i == 0)
//                    i++;
//                else
//                    return 1;
//        } while (*text != '\0' && (*text++ == c || c == '.'));
//        return 0;
//    }
    final private int matchplus(char c, int r, int t) {
        int i = 0;
        do { //a matches one or more instances
            if (matchhere(r, r) == 1) {
                if (i == 0) {
                    i++;
                } else {
                    return 1;
                }
            }
        } while ((t != this.text.limit()) && ((this.text.charAt(t++) == c) || c == '.'));
        return 0;
    }
}
