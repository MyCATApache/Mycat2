package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface PatternDFG {
    int addRule(Iterator<Seq> format);

    Matcher getMatcher();

    static class DFGImpl implements PatternDFG {
        private final State rootState = new State(0);
        Map<String, Position> variables = new HashMap<>();
        int identifierGenerator = 0;

        @Override
        public int addRule(Iterator<Seq> format) {
            State state = this.rootState;
            for (; format.hasNext();) {
                Seq token = format.next();
                if ("{".equals(token.getSymbol())) {
                    format.hasNext();
                    String name = format.next().getSymbol();
                    if (!variables.containsKey(name)) variables.put(name, null);
                    else throw new UnsupportedOperationException();
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
            state.end(identifierGenerator++);
            return state.id;
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
            private int id = Integer.MIN_VALUE;

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

            public State accept(Seq token, int startOffset, int endOffset, PositionRecorder map) {
                State state = null;
                if (success != null) {
                    state = success.get(token);
                }
                if (state != null) {
                    return state;
                }
                if (matcher != null) {
                    State accept = matcher.accept(token, startOffset, endOffset, map);
                    if (accept != null) {
                        map.endRecordName(name);
                        return accept;
                    } else {
                        map.startRecordName(name, startOffset);
                        map.record(endOffset);
                        return this;
                    }
                }
                return null;
            }

            public void end(int id) {
                this.id = id;
            }

            public boolean isEnd() {
                return this.id > -1;
            }
        }
    }

    public class MatcherImpl implements Matcher {
        private final DFGImpl.State rootState;
        private final PositionRecorder context = new PositionRecorder();
        private DFGImpl.State state;

        public MatcherImpl(DFGImpl.State state) {
            this.rootState = state;
        }

        public boolean accept(Seq token) {
            if (this.state == null) return false;
            DFGImpl.State orign = this.state;
            DFGImpl.State state = this.state.accept(token, token.getStartOffset(), token.getEndOffset(), context);
            boolean b = (orign) != state;
            this.state = state;
            return b;
        }

        public boolean acceptAll() {
            return state != null && state.isEnd();
        }

        @Override
        public int id() {
            return acceptAll()?state.id:Integer.MIN_VALUE;
        }

        @Override
        public Map<String, Position> context() {
            return context.map;
        }

        @Override
        public void reset() {
            this.state = rootState;
        }
    }
}