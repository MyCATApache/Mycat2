package io.mycat.sqlparser.util.simpleParser;

import com.google.common.base.Charsets;

import java.util.Arrays;

public class UTF8Util {

    public static void main(String[] args) {
        String message = "哈糊糊";
        byte[] bytes = message.getBytes(Charsets.UTF_8);
        int count = 0;
        int length = bytes.length;
        boolean needFill = false;
        byte fillCode = 'm';
        boolean isAscii = true;
        boolean debug = false;
        boolean b = fixUTF8(bytes, count, length, needFill, fillCode);
        System.out.println("count:" + count);
        System.out.println(new String(bytes, Charsets.UTF_8));
    }

    public static boolean fixUTF8(byte[] bytes, char fillCode) {
        return fixUTF8(bytes, 0, bytes.length, true, (byte) fillCode);
    }

    public static boolean isASCII(byte[] bytes) {
        return fixUTF8(bytes, 0, bytes.length, false, (byte) 0);
    }

    public static boolean isAscii(byte c) {
        return (c & 0xff) <= 0x007F;
    }
    public static boolean isAscii(int c) {
        return (c &0xff) <= 0x007F;
    }
    public static boolean isAscii(byte[] bytes, int offset) {
        return (bytes[offset]&0xff) <= 0x007F;
    }

    public static boolean fixUTF8(byte[] bytes, int offset, int length, boolean needFill, byte fillCode) {
        boolean isAscii = true;
        boolean debug = false;
        int count = 0;
        for (; offset < length; ) {
            int aByte = Byte.toUnsignedInt(bytes[offset]);
            if (aByte <= 0x007F) {
                if (debug) {
                    System.out.println("1 bytes ");
                }
                offset += 1;
            } else {
                isAscii = false;
                if (aByte <= 0x07FF) {
                    if (debug) {
                        System.out.println("2 bytes ");
                    }
                    if (needFill) {
                        Arrays.fill(bytes, offset, offset + 3, fillCode);
                    }
                    offset += 3;
                } else if (aByte <= 0xFFFF) {
                    System.out.println("3 bytes");
                    if (needFill) {
                        Arrays.fill(bytes, offset, offset + 4, fillCode);
                    }
                    offset += 4;
                } else if (aByte <= 0x1FFFFF) {
                    if (debug) {
                        System.out.println("4 bytes");
                    }
                    if (needFill) {
                        Arrays.fill(bytes, offset, offset + 5, fillCode);
                    }
                    offset += 5;
                } else if (aByte <= 0x3FFFFFF) {
                    if (debug) {
                        System.out.println("5 bytes ");
                    }
                    if (needFill) {
                        Arrays.fill(bytes, offset, offset + 6, fillCode);
                    }
                    offset += 6;
                } else if (aByte <= 0x7FFFFFFF) {
                    if (debug) {
                        System.out.println("6 bytes ");
                    }
                    if (needFill) {
                        Arrays.fill(bytes, offset, offset + 7, fillCode);
                    }
                    offset += 7;
                }
            }
            count++;
        }
        return isAscii;
    }
}