package io.mycat.sqlparser.util.simpleParser2;

import com.sun.source.tree.ExpressionTree;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public interface DFG {
    void addRule(Iterator<Token> format);

    boolean accept(Token token);

    static class DFGImpl implements DFG {
        Map<Token, DFGImpl> status = new HashMap<>();
        DFGImpl curDFG = null;

        @Override
        public void addRule(Iterator<Token> format) {
            Objects.requireNonNull(format);
            while (format.hasNext()) {
                insert(format.next());
            }
        }

        private void insert(Token token) {

        }

        @Override
        public boolean accept(Token token) {
            if (curDFG == null) {
                curDFG = status.get(token);
            }
            return curDFG != null;
        }

        public boolean isTerminal(){
           return curDFG.isTerminal();
        }
    }

    public static void main(String[] args) {
        DFG dfg = new DFGImpl();

    }
}