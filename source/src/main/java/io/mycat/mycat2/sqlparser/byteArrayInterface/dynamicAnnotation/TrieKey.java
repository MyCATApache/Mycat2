package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

public class TrieKey {
        int type;
        int intHash;
        String text;

    public TrieKey(int type, int intHash, String text) {
        this.type = type;
        this.intHash = intHash;
        this.text = text;
    }

    public int getIntHash() {
        return intHash;
    }

    public void setIntHash(int intHash) {
        this.intHash = intHash;
    }

    public int getType() {
           return type;
       }

       public void setType(int type) {
           this.type = type;
       }



       public String getText() {
           return text;
       }

       public void setText(String text) {
           this.text = text;
       }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TrieKey trieKey = (TrieKey) o;

        if (type != trieKey.type) return false;
        if (intHash != trieKey.intHash) return false;
        return text != null ? text.equals(trieKey.text) : trieKey.text == null;
    }

    @Override
    public int hashCode() {
        int result = type;
        result = 31 * result + intHash;
        result = 31 * result + (text != null ? text.hashCode() : 0);
        return result;
    }
}