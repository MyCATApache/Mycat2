package io.mycat.sqlparser.util.simpleParser2;

import java.text.MessageFormat;

public class GroupPatternException extends UnsupportedOperationException {
    public GroupPatternException(String pattern, Object... args) {
        super(MessageFormat.format(pattern, args));
    }

    public GroupPatternException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class NameAdjacentException extends GroupPatternException {
        public NameAdjacentException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameAmbiguityException extends GroupPatternException {
        public NameAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameLocationAmbiguityException extends GroupPatternException {
        public NameLocationAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameSyntaxException extends GroupPatternException {
        public NameSyntaxException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
    public static class NonASCIICharsetConstTokenException extends GroupPatternException {
        public NonASCIICharsetConstTokenException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
    public static class TooLongConstTokenException extends GroupPatternException {
        public TooLongConstTokenException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
}