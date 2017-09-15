package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DynamicAnnotationKey{
    final String schemaName;
    final SQLType sqlType;
    final String[] tables;
    final String matchName;

    public DynamicAnnotationKey(String schemaName, SQLType sqlType, String[] tables, String matchName) {
        this.schemaName = schemaName;
        this.sqlType = sqlType;
        this.tables = tables;
        this.matchName = matchName;
        ArrayList<String> a=new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DynamicAnnotationKey that = (DynamicAnnotationKey) o;

      if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
      if (sqlType != that.sqlType) return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      if (!Arrays.equals(tables, that.tables)) return false;
      return matchName != null ? matchName.equals(that.matchName) : that.matchName == null;
    }

    @Override
    public int hashCode() {
      int result = schemaName != null ? schemaName.hashCode() : 0;
      result = 31 * result + (sqlType != null ? sqlType.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(tables);
      result = 31 * result + (matchName != null ? matchName.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
        return "DynamicAnnotationKey{" +
                "schemaName='" + schemaName + '\'' +
                ", sqlType=" + sqlType +
                ", tables=" + Arrays.toString(tables) +
                ", matchName='" + matchName + '\'' +
                '}';
    }
}