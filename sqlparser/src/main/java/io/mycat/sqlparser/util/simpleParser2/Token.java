package io.mycat.sqlparser.util.simpleParser2;

import lombok.ToString;

@ToString
public class Token<T> implements Cloneable{
    int hash;
    String symbol;
    T attr;
    int start;
    int end;

    public Token(int hash, String symbol, T attr) {
        this.hash = hash;
        this.symbol = symbol;
        this.attr = attr;
    }

    boolean isSymbol() {
        return attr != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Token<?> token = (Token<?>) o;

        if (hash != token.hash) return false;
        if (symbol != null ? !symbol.equals(token.symbol) : token.symbol != null) return false;
        return attr != null ? attr.equals(token.attr) : token.attr == null;
    }

    @Override
    public int hashCode() {
        int result = hash;
        result = 31 * result + (symbol != null ? symbol.hashCode() : 0);
        result = 31 * result + (attr != null ? attr.hashCode() : 0);
        return result;
    }

    @Override
    protected Token clone() throws CloneNotSupportedException {
        return (Token) super.clone();
    }
}