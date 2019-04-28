package io.mycat.beans.mysql;

import java.util.Objects;

public class MySQLCharset {
    final String charset;
    final int charsetIndex;
    final String setCharsetSQL;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MySQLCharset that = (MySQLCharset) o;
        return charsetIndex == that.charsetIndex &&
                Objects.equals(charset, that.charset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charset, charsetIndex);
    }

    public String getSetCharsetCmd() {
        return setCharsetSQL;
    }

    public String getCharset() {
        return charset;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public MySQLCharset(String charset, int charsetIndex) {
        this.charset = charset;
        this.charsetIndex = charsetIndex;
        this.setCharsetSQL = "SET names " + charset + ";";
    }
}
