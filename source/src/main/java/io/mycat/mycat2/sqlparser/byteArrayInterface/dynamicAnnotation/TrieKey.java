package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

public class TrieKey {
        int type;
        long longHash;
        String text;

       public TrieKey(int type, long longHash, String text) {
           this.type = type;
           this.longHash = longHash;
           this.text = text;
       }

       public int getType() {
           return type;
       }

       public void setType(int type) {
           this.type = type;
       }

       public long getLongHash() {
           return longHash;
       }

       public void setLongHash(long longHash) {
           this.longHash = longHash;
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

           TrieKey key = (TrieKey) o;

           if (type != key.type) return false;
           if (longHash != key.longHash) return false;
           return text != null ? text.equals(key.text) : key.text == null;
       }

       @Override
       public int hashCode() {
           int result = type;
           result = 31 * result + (int) (longHash ^ (longHash >>> 32));
           result = 31 * result + (text != null ? text.hashCode() : 0);
           return result;
       }

       @Override
       public String toString() {
           return "TrieKey{" +
                   "type=" + type +
                   ", longHash=" + longHash +
                   ", text='" + text + '\'' +
                   '}';
       }
   }