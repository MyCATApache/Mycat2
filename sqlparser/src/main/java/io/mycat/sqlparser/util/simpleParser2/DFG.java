package io.mycat.sqlparser.util.simpleParser2;

import lombok.Data;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface DFG {
    void addRule(Iterator<Seq> format);

    Matcher getMatcher();

    static class DFGImpl implements DFG {
        private final State rootState = new State(0);
        private int index;
        private int length[] = new int[8192];
        Map<String, Position> variables = new HashMap<>();

        @Override
        public void addRule(Iterator<Seq> format) {
            State state = this.rootState;
            int length = 0;
            for (; format.hasNext(); ++length) {
                Seq token = format.next();
                if (token == null) continue;
                if ("{".equals(token.getSymbol())) {
                    format.hasNext();
                    String name = format.next().getSymbol();
                    variables.put(name, null);
                    state.addWildcard(name, new State(state.depth + 1));
                    format.hasNext();
                    Seq last = format.next();
                    if ("}".equals(last.getSymbol())) {
                        state = state.matcher;
                    } else {
                        throw new UnsupportedOperationException();
                    }
                } else {
                    state = state.addState(token);
                }
            }
            this.length[++index] = length;
        }

        @Override
        public Matcher getMatcher() {
            return new MatcherImpl(rootState);
        }

        public static class State {
            final int depth;
            private String name;
            private HashMap<Seq, State> success;
            private State matcher;

            public State(int depth) {
                this.depth = depth;
            }

            public State addState(Seq next) {
                if (success == null) success = new HashMap<>();
                if (success.containsKey(next)) {
                    return success.get(next);
                } else {
                    State state = new State(depth + 1);
                    success.put(next, state);
                    return state;
                }
            }

            public void addWildcard(String name, State matcher) {
                if (success == null && this.name == null) {
                    this.name = name;
                    this.matcher = matcher;
                } else throw new UnsupportedOperationException();
            }

            public State accept(Seq token, int startOffset, int endOffset, DFGImpl.PositionRecorder map) {
                if (success != null && name == null) {
                    return success.get(token);
                } else {
                    if (name != null) {
                        map.startRecordName(name, startOffset);
                    }
                    if (matcher != null) {
                        State accept = matcher.accept(token, startOffset, endOffset, map);
                        if (accept != null && name != null) {
                            if (accept.success == null && accept.matcher == null) {
                                map.record(endOffset);
                            }
                            map.endRecordName(name);
                            return accept;
                        }
                        map.record(endOffset);
                    } else if (name != null) {
                        map.record(endOffset);
                        map.endRecordName(name);
                    }
                    return this;
                }
            }
        }

        public static class PositionRecorder {
            final Map<String, Position> map = new HashMap<>();
            Position currentPosition;

            public void startRecordName(String name, int startOffset) {
                if (currentPosition == null) {
                    currentPosition = new Position();
                    currentPosition.start = Integer.MAX_VALUE;
                }
                currentPosition.start = Math.min(currentPosition.start, startOffset);
            }

            public void record(int endOffset) {
                if (currentPosition != null) {
                    currentPosition.end = Math.max(currentPosition.end, endOffset);
                }
            }

            public void endRecordName(String name) {
                if (currentPosition != null) {
                    map.put(name, currentPosition);
                    currentPosition = null;
                }
            }
        }

        @Data
        public static class Position {
            int start;
            int end;
        }
    }

    public interface Matcher {
        boolean accept(Seq token);

        Map<String, String> values(byte[] bytes1);

        public boolean acceptAll();
    }

    public interface Seq {
        String getSymbol();

        int getStartOffset();

        int getEndOffset();
    }

    public class MatcherImpl implements Matcher {
        private DFGImpl.State state;
        private final DFGImpl.PositionRecorder map = new DFGImpl.PositionRecorder();

        public MatcherImpl(DFGImpl.State state) {
            this.state = state;
        }

        public boolean accept(Seq token) {
            if (this.state == null) return false;
            int startOffset = token.getStartOffset();
            int endOffset = token.getEndOffset();
            return (this.state = state.accept(token, startOffset, endOffset, map)) != null;
        }

        public boolean acceptAll() {
            return state != null && state.matcher == null && state.success == null;
        }

        @Override
        public Map<String, String> values(byte[] bytes1) {
            Map<String, String> res = new HashMap<>();
            for (Map.Entry<String, DFGImpl.Position> entry : map.map.entrySet()) {
                String key = entry.getKey();
                DFGImpl.Position value = entry.getValue();
                res.put(key, new String(bytes1, value.start, value.end - value.start, Charset.defaultCharset()));
            }
            return res;
        }
    }
}