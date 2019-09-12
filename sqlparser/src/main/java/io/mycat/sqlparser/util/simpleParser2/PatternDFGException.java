package io.mycat.sqlparser.util.simpleParser2;

import java.text.MessageFormat;

public class PatternDFGException extends UnsupportedOperationException {
    public PatternDFGException(String pattern, Object... args) {
        super(MessageFormat.format(pattern, args));
    }

    public PatternDFGException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class NameAdjacentException extends PatternDFGException {
        public NameAdjacentException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameAmbiguityException extends PatternDFGException {
        public NameAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameLocationAmbiguityException extends PatternDFGException {
        public NameLocationAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameSyntaxException extends PatternDFGException {
        public NameSyntaxException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
}