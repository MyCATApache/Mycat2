package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface PatternDFG {
    int addRule(Iterator<Seq> format);

    Matcher getMatcher();

    static class DFGImpl implements PatternDFG {
        private final State rootState = new State(0);
        private final Map<String, Position> variables = new HashMap<>();
        int identifierGenerator = 0;

        public DFGImpl(int identifierGenerator) {
            this.identifierGenerator = identifierGenerator;
        }

        public DFGImpl() {
            this(0);
        }

        @Override
        public int addRule(Iterator<Seq> format) {
            State state = this.rootState;
            State lastState = null;
            String lastName = null;
            for (; format.hasNext(); ) {
                lastState = state;
                Seq token = format.next();
                if ("{".equals(token.getSymbol())) {
                    if (!format.hasNext()) throw new GroupPatternException.NameSyntaxException("'{' name ends early");
                    String name = format.next().getSymbol().trim();
                    if (lastName != null)
                        throw new GroupPatternException.NameAdjacentException("'{'{0}'}' '{'{1}'}' is not allowed", lastName, name);
                    if (!variables.containsKey(name)) {
                        variables.put(name, null);
                    } else if ((!name.equals(state.name))) {
                        throw new GroupPatternException.NameAmbiguityException("'{'{0}'}' has already existed", name);
                    }
                    state.addWildcard(name, new State(state.depth + 1));
                    if (!format.hasNext())
                        throw new GroupPatternException.NameSyntaxException("'{'{0} ends early", name);
                    Seq last = format.next();
                    if ("}".equals(last.getSymbol())) {
                        if (state.matcher != null) {
                            state = state.matcher;
                        }
                        lastName = name;
                    } else
                        throw new GroupPatternException.NameSyntaxException("'{'{0} {1}   The name can only identify one", name, last.getSymbol());
                } else {
                    state = state.addState(token);
                    lastName = null;
                }
            }
            int i = identifierGenerator++;
            if (lastState != null && lastState.name != null && state.name == null && state.success == null && state.matcher == null) {
                state.end(i);
                state = lastState;
            }
            if (!state.isEnd()){
                state.end(i);
            }
            return i;
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
            private int constId = Integer.MIN_VALUE;
            private int matcherId = Integer.MIN_VALUE;
            private boolean matcherEnd = false;

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
                if (this.name == null) {
                    this.name = name;
                    this.matcher = matcher;
                } else if (this.name.equals(name)) {

                } else
                    throw new GroupPatternException.NameLocationAmbiguityException("'{' {0} '}' '{' {1} '}' are ambiguous", this.name, name);
            }

            public State accept(Seq token, int startOffset, int endOffset, MatcherImpl map) {
                State state = null;
                if (success != null) {
                    state = success.get(token);
                    if (state != null) {
                        return state;
                    }
                }

                if (name != null) {
                    State accept = matcher.accept(token, startOffset, endOffset, map);
                    if (accept != null) {
                        return accept;
                    } else {
                        map.context.startRecordName(name, startOffset);
                        map.context.record(endOffset);
                        return this;
                    }
                }
                return null;
            }

            public void end(int id) {
                if (!matcherEnd) {
                    this.constId = id;
                    this.matcherEnd = true;
                }
            }

            public boolean isEnd() {
                return matcherEnd;
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
            DFGImpl.State state = this.state.accept(token, token.getStartOffset(), token.getEndOffset(), this);
            boolean b = ((orign) != state);
            this.state = state;
            return b;
        }

        public boolean acceptAll() {
            return state != null && state.isEnd();
        }

        @Override
        public int id() {
            return acceptAll() ? state.constId : Integer.MIN_VALUE;
        }

        @Override
        public Map<String, Position> context() {
            return context.map;
        }

        @Override
        public void reset() {
            this.state = rootState;
            this.context.map.clear();
            this.context.name = null;
            this.context.currentPosition = null;
        }
    }
}