/**
 * Copyright (C) <2020>  <chen junwen>
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