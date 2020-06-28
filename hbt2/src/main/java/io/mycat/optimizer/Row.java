package io.mycat.optimizer;

import lombok.*;

import java.util.Arrays;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Getter
@EqualsAndHashCode
public class Row {
    public Object[] values;

    public Row compose(Row right) {
        Row row = new Row();
        int newLength = this.values.length + right.values.length;
        row.values = new Object[newLength];
        System.arraycopy( this.values,0,  row.values,0,this.values.length);
        System.arraycopy(right.values,0,  row.values,this.values.length,right.values.length);
        return row;
    }

    public Object getObject(int i) {
        return values[i];
    }
    public static Row create(int size){
        Row row = new Row();
        row.values = new Object[size];
        return row;
    }

    public void set(int i, Object object) {
        values[i] = object;
    }

    public int size() {
        return values.length;
    }
}