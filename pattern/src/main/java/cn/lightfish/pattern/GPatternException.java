/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import java.text.MessageFormat;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternException extends UnsupportedOperationException {
    public GPatternException(String pattern, Object... args) {
        super(MessageFormat.format(pattern, args));
    }

    public GPatternException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class NameAdjacentException extends GPatternException {
        public NameAdjacentException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameAmbiguityException extends GPatternException {
        public NameAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameLocationAmbiguityException extends GPatternException {
        public NameLocationAmbiguityException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class NameSyntaxException extends GPatternException {
        public NameSyntaxException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
    public static class NonASCIICharsetConstTokenException extends GPatternException {
        public NonASCIICharsetConstTokenException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
    public static class TooLongConstTokenException extends GPatternException {
        public TooLongConstTokenException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class ConstTokenHashConflictException extends GPatternException {
        public ConstTokenHashConflictException(String pattern, Object... args) {
            super(pattern, args);
        }
    }

    public static class PatternConflictException extends GPatternException {
        public PatternConflictException(String pattern, Object... args) {
            super(pattern, args);
        }
    }
}