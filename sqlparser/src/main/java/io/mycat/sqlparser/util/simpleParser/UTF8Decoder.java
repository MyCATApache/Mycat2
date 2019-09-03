package io.mycat.sqlparser.util.simpleParser;

import com.google.common.base.Charsets;

import java.nio.charset.Charset;

public class UTF8Decoder {
    public static void main(String[] args) {
        String message = "春眠不觉晓，处处闻啼鸟。a";
        byte[] bytes = message.getBytes(Charsets.UTF_8);
        int count = 0;
        for (int i = 0; i < bytes.length; i++) {
            int aByte =Byte.toUnsignedInt(bytes[i]) ;
            if (aByte <= 0x007F) {
                System.out.println("1 bytes "+new String(bytes,i,1, Charsets.UTF_8));
            } else {
                if (aByte <= 0x07FF) {
                    System.out.println("2 bytes "+new String(bytes,i,2,Charsets.UTF_8));
                    i += 2;
                } else if (aByte <= 0xFFFF) {
                    System.out.println("3 bytes"+new String(bytes,i,3,Charsets.UTF_8));
                    i += 3;
                } else if (aByte <= 0x1FFFFF) {
                    System.out.println("4 bytes"+new String(bytes,i,4,Charsets.UTF_8));
                    i += 4;
                } else if (aByte <= 0x3FFFFFF) {
                    System.out.println("5 bytes "+new String(bytes,i,5,Charsets.UTF_8));
                    i += 5;
                } else if (aByte <= 0x7FFFFFFF) {
                    System.out.println("6 bytes "+new String(bytes,i,6,Charsets.UTF_8));
                    i += 6;
                }
            }
            count++;
        }
        System.out.println("count:"+count);
    }
}