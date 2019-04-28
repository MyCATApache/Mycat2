package io.mycat.beans.mysql;

public class MySQLCollation {
    String collatioNname;
    String charsetName;
    int id;
    boolean isDefault;
    boolean compiled;
    int sortLen;

    public String getCollatioNname() {
        return collatioNname;
    }

    public void setCollatioNname(String collatioNname) {
        this.collatioNname = collatioNname;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean aDefault) {
        isDefault = aDefault;
    }

    public boolean isCompiled() {
        return compiled;
    }

    public void setCompiled(boolean compiled) {
        this.compiled = compiled;
    }

    public int getSortLen() {
        return sortLen;
    }

    public void setSortLen(int sortLen) {
        this.sortLen = sortLen;
    }
}
