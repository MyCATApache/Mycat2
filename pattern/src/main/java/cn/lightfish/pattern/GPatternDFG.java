/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public interface GPatternDFG {
    int addRule(Iterator<GPatternSeq> format);

    GPatternMatcher getMatcher(GPattern gPattern);

    Map<String, GPatternPosition> getVariables();

    final class DFGImpl implements GPatternDFG {
        private final State rootState = new State(0);
        private final Map<String, GPatternPosition> variables = new HashMap<>();
        int identifierGenerator = 0;

        public DFGImpl(int identifierGenerator) {
            this.identifierGenerator = identifierGenerator;
        }

        public DFGImpl() {
            this(0);
        }

        @Override
        public int addRule(Iterator<GPatternSeq> format) {
            State state = this.rootState;
            State lastState = null;
            String lastName = null;
            GPatternSeq nextToken = null;
            State nextState = null;
            for (; format.hasNext(); ) {
                lastState = state;
                GPatternToken token = (GPatternToken) format.next();
                if ("{".equals(token.getSymbol())) {
                    if (!format.hasNext()) throw new GPatternException.NameSyntaxException("'{' name ends early");
                    String name = format.next().getSymbol().trim();
                    if (lastName != null)
                        throw new GPatternException.NameAdjacentException("'{'{0}'}' '{'{1}'}' is not allowed", lastName, name);
                    if (!variables.containsKey(name)) {
                        variables.put(name, null);
                    } else if ((!name.equals(state.name))) {
                        throw new GPatternException.NameAmbiguityException("'{'{0}'}' has already existed", name);
                    }
                    state.addWildcard(name, new State(state.depth + 1));
                    if (!format.hasNext())
                        throw new GPatternException.NameSyntaxException("'{'{0} ends early", name);
                    GPatternSeq last = format.next();
                    if ("}".equals(last.getSymbol())) {
                        if (state.matcher != null) {
                            state = state.matcher;
                        }
                        lastName = name;
                    } else
                        throw new GPatternException.NameSyntaxException("'{'{0} {1}   The name can only identify one", name, last.getSymbol());
                } else {
                    state = state.addState(token);
                    nextToken = token;
                    nextState = state;
                    lastName = null;
                }
            }
            int id = identifierGenerator++;
            boolean set = false;//test 19 test 30
            if (lastState != null && lastState.name != null && state.name == null && state.success.isEmpty() && state.matcher == null) {
                state.end(id);
                id = state.id;
                state = lastState;
                set = true;
            }
            if (!state.isEnd()) {
                state.end(id);
                id = state.id;
                set = true;
            }
//
            ////优化
            if (state.success.size() == 1 && nextToken != null) {
                state.nextToken = nextToken;
                state.nextState = nextState;
                state.success.clear();
            }

            return set ? id : state.id;
        }

        @Override
        public GPatternMatcher getMatcher(GPattern gPattern) {
            return new MatcherImpl(this, gPattern.getUtf8Lexer());
        }

        @Override
        public Map<String, GPatternPosition> getVariables() {
            return variables;
        }


        public static class State {
            final int depth;
            public GPatternSeq nextToken;
            public State nextState;
            private String name;
            private final Object2ObjectOpenHashMap<GPatternToken, State> success = new Object2ObjectOpenHashMap<GPatternToken, State>();
            private State matcher;
            private int id = Integer.MIN_VALUE;
            private boolean end = false;

            public State(int depth) {
                this.depth = depth;
            }

            public State addState(GPatternToken next) {
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
                    throw new GPatternException.NameLocationAmbiguityException("'{' {0} '}' '{' {1} '}' are ambiguous", this.name, name);
            }

            public State accept(GPatternToken token, int startOffset, int endOffset, MatcherImpl map) {
                if (nextToken != null) {
                    if (token.fastEquals(nextToken)) {
                        return nextState;
                    }
                }
                if (!success.isEmpty()) {
                    State state = success.get(token);
                    if (state != null) {
                        return state;
                    }
                }
                if (name != null) {
                    return wildCardMatch(token, startOffset, endOffset, map);
                }
                return null;
            }

            private State wildCardMatch(GPatternToken token, int startOffset, int endOffset, MatcherImpl map) {
                State accept = matcher.accept(token, startOffset, endOffset, map);
                if (accept != null) {
                    return accept;
                } else {
                    map.context.startRecordName(name, startOffset);
                    map.context.record(endOffset);
                    return this;
                }
            }

            public void end(int id) {
                if (!end) {
                    this.id = id;
                    this.end = true;
                }
            }

            public boolean isEnd() {
                return end;
            }
        }
    }

    final class MatcherImpl implements GPatternMatcher {
        private final DFGImpl.State rootState;
        private final GPositionRecorder context;
        private GPatternUTF8Lexer lexer;
        private DFGImpl.State state;

        public MatcherImpl(DFGImpl dfg, GPatternUTF8Lexer lexer) {
            this.rootState = dfg.rootState;
            this.context = new GPositionRecorder(dfg.variables);
            this.lexer = lexer;
        }

        public boolean accept(GPatternToken token) {
            if (this.state == null) return false;
            DFGImpl.State orign = this.state;
            this.state = this.state.accept(token, token.getStartOffset(), token.getEndOffset(), this);
            return ((orign) != state);
        }

        public boolean acceptAll() {
            return state != null && state.isEnd();
        }

        @Override
        public int id() {
            return acceptAll() ? state.id : Integer.MIN_VALUE;
        }

        @Override
        public Map<String, GPatternPosition> positionContext() {
            return context.map;
        }

        @Override
        public String getName(String name) {
            GPatternPosition gPatternPosition = context.map.get(name);
            if (gPatternPosition == null) {
                return null;
            } else {
                return lexer.getString(gPatternPosition.getStart(), gPatternPosition.getEnd());
            }
        }

        @Override
        public Map<String, String> namesContext() {
            return namesContext(new HashMap<>());
        }

        @Override
        public Map<String, String> namesContext(Map<String, String> res) {
            for (Map.Entry<String, GPatternPosition> entry : this.positionContext().entrySet()) {
                GPatternPosition value = entry.getValue();
                if (value.end < 0) {
                    continue;
                }
                res.put(entry.getKey(), lexer.getString(value.start, value.end));
            }
            return res;
        }

        @Override
        public GPatternUTF8Lexer lexer() {
            return lexer;
        }

        @Override
        public boolean accept(GPatternSeq token) {
            return accept((GPatternToken) token);
        }

        @Override
        public void reset() {
            this.state = rootState;
            this.context.name = null;
            this.context.currentPosition = null;
            for (GPatternPosition value : this.context.map.values()) {
                value.end = -1;
                value.start = -1;
            }

        }
    }
}