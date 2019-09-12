package io.mycat.sqlparser.util.simpleParser2;

import lombok.ToString;

@ToString
public class TokenImpl implements Cloneable, Seq {
    int hash;
    private String symbol;
    Object attr;
    int startOffset = -1;
    int endOffset = -1;
    UTF8Lexer lexer;

    public TokenImpl(int hash, String symbol, Object attr) {
        this.hash = hash;
        this.symbol = symbol;
        this.attr = attr;
    }

    public String getSymbol() throws NullPointerException {
        if (symbol != null){
           return symbol;
        }
        if (lexer!=null){
            return lexer.getString(startOffset, endOffset);
        }
        return null;
    }

    @Override
    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public int getEndOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenImpl token = (TokenImpl) o;
        if (hash != token.hash) return false;
        return symbol != null ? symbol.equals(token.symbol) : token.symbol == null;
    }

    @Override
    public int hashCode() {
        int result = hash;
        result = 31 * result + (symbol != null ? symbol.hashCode() : 0);
        return result;
    }

    @Override
    protected TokenImpl clone() throws CloneNotSupportedException {
        return (TokenImpl) super.clone();
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setLexer(UTF8Lexer lexer) {
        this.lexer = lexer;
    }
}