package io.mycat.sqlparser.util.simpleParser2;

import jdk.nashorn.internal.parser.Token;

import java.text.ParseException;

public interface LexerInterface {
    Token read() throws ParseException;

    void skip(String token) throws ParseException;

    void skip(Token token) throws ParseException;

    Token peek(int i) throws ParseException;

    boolean isToken(String text) throws ParseException;
}
