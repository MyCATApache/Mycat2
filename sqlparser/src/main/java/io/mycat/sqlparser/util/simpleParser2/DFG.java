package io.mycat.sqlparser.util.simpleParser2;

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
            state.end();
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
            private boolean end = false;

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

            public void end() {
                this.end = true;
            }

            public boolean isEnd() {
                return end;
            }
        }
    }

    public class MatcherImpl implements Matcher {
        private DFGImpl.State rootState;
        private DFGImpl.State state;
        private final PositionRecorder context = new PositionRecorder();

        public MatcherImpl(DFGImpl.State state) {
            this.rootState = state;
        }

        public boolean accept(Seq token) {
            if (this.state == null) return false;
            DFGImpl.State orign = this.state;
            DFGImpl.State state = this.state.accept(token, token.getStartOffset(), token.getEndOffset(), context);
            System.out.println(orign + "->" + state);
            boolean b = (orign) != state;
            this.state = state;
            return b;
        }

        public boolean acceptAll() {
            return state != null && state.end;
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