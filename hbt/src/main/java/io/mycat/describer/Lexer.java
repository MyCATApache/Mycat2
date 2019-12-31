package io.mycat.describer;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.Token;

public class Lexer {
    final com.alibaba.fastsql.sql.parser.Lexer lexer;

    public Lexer(String text) {
        this.lexer = SQLParserUtils.createLexer(text, DbType.mysql);
    }

    public void nextToken() {
        this.lexer.nextToken();
    }

    public String info() {
        return this.lexer.info();
    }

    public Number integerValue() {
        return this.lexer.integerValue();
    }


    public Number decimalValue() {
        return this.lexer.decimalValue();
    }

    public String stringVal() {
        return this.lexer.stringVal();
    }

    public boolean isEOF() {
        return this.lexer.isEOF();
    }

    public boolean identifierEquals(String let) {
        return this.lexer.identifierEquals(let);
    }

    public Token token() {
        return this.lexer.token();
    }

    public String tokenString() {
        Token token = this.lexer.token();
        if (token.name != null) {
            return token.name.toLowerCase();
        } else {
            return lexer.stringVal();
        }
    }

    public String hexString() {
        return this.lexer.hexString();
    }
}