package io.mycat.expression;

public class Main {
    public static void main(String[] args) {
        UnsafeRowWriter writer = new UnsafeRowWriter(5);
        writer.write(1,1);
        writer.write(2,new byte[]{13,23,4});
        writer.write(3,new byte[8193]);
        UnsafeRow row = writer.getRow();
        int anInt = row.getInt(1);
    }
}
