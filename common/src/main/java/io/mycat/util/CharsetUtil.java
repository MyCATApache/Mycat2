/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.util;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author mycat
 */
public class CharsetUtil {

  public static final MycatLogger LOGGER = MycatLoggerFactory
                                          .getLogger(CharsetUtil.class);
  private static final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
  private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

  public static void init(String root) {
    INDEX_TO_CHARSET.put(-1, "UTF-8");//客户端连接-1跟随连接用的编码
    // index_to_charset.properties
    INDEX_TO_CHARSET.put( 1, "Big5");
    INDEX_TO_CHARSET.put( 2, "ISO8859_2");
    INDEX_TO_CHARSET.put( 3, "ISO8859_1");
    INDEX_TO_CHARSET.put( 4, "Cp437");
    INDEX_TO_CHARSET.put( 5, "ISO8859_1");
    INDEX_TO_CHARSET.put( 6, "ISO8859_1");
    INDEX_TO_CHARSET.put( 7, "KOI8_R");
    INDEX_TO_CHARSET.put( 8, "ISO8859_1");
    INDEX_TO_CHARSET.put( 9, "ISO8859_2");
    INDEX_TO_CHARSET.put( 10, "ISO8859_1");
    INDEX_TO_CHARSET.put( 11, "US-ASCII");
    INDEX_TO_CHARSET.put( 12, "EUC_JP");
    INDEX_TO_CHARSET.put( 13, "SJIS");
    INDEX_TO_CHARSET.put( 14, "Cp1251");
    INDEX_TO_CHARSET.put( 15, "ISO8859_1");
    INDEX_TO_CHARSET.put( 16, "HEBREW");
    INDEX_TO_CHARSET.put( 18, "TIS620");
    INDEX_TO_CHARSET.put( 19, "EUCKR");
    INDEX_TO_CHARSET.put( 20, "ISO8859_7");
    INDEX_TO_CHARSET.put( 21, "ISO8859_2");
    INDEX_TO_CHARSET.put( 22, "KOI8_R");
    INDEX_TO_CHARSET.put( 23, "Cp1251");
    INDEX_TO_CHARSET.put( 24, "GB2312");
    INDEX_TO_CHARSET.put( 25, "GREEK");
    INDEX_TO_CHARSET.put( 26, "Cp1250");
    INDEX_TO_CHARSET.put( 27, "ISO8859_2");
    INDEX_TO_CHARSET.put( 28, "GBK");
    INDEX_TO_CHARSET.put( 29, "Cp1257");
    INDEX_TO_CHARSET.put( 30, "LATIN5");
    INDEX_TO_CHARSET.put( 31, "ISO8859_1");
    INDEX_TO_CHARSET.put( 32, "ISO8859_1");
    INDEX_TO_CHARSET.put( 33, "UTF-8");
    INDEX_TO_CHARSET.put( 34, "Cp1250");
    INDEX_TO_CHARSET.put( 35, "UTF-16BE");
    INDEX_TO_CHARSET.put( 36, "Cp866");
    INDEX_TO_CHARSET.put( 37, "Cp895");
    INDEX_TO_CHARSET.put( 38, "MacCentralEurope");
    INDEX_TO_CHARSET.put( 39, "MacRoman");
    INDEX_TO_CHARSET.put( 40, "LATIN2");
    INDEX_TO_CHARSET.put( 41, "ISO8859_7");
    INDEX_TO_CHARSET.put( 42, "ISO8859_7");
    INDEX_TO_CHARSET.put( 43, "MacCentralEurope");
    INDEX_TO_CHARSET.put( 44, "Cp1250");
    INDEX_TO_CHARSET.put( 45, "UTF-8");
    INDEX_TO_CHARSET.put( 47, "ISO8859_1");
    INDEX_TO_CHARSET.put( 48, "ISO8859_1");
    INDEX_TO_CHARSET.put( 49, "ISO8859_1");
    INDEX_TO_CHARSET.put( 50, "Cp1251");
    INDEX_TO_CHARSET.put( 51, "Cp1251");
    INDEX_TO_CHARSET.put( 52, "Cp1251");
    INDEX_TO_CHARSET.put( 53, "MacRoman");
    INDEX_TO_CHARSET.put( 57, "Cp1256");
    INDEX_TO_CHARSET.put( 58, "Cp1257");
    INDEX_TO_CHARSET.put( 59, "Cp1257");
    INDEX_TO_CHARSET.put( 63, "US-ASCII");
    INDEX_TO_CHARSET.put( 64, "ISO8859_2");
    INDEX_TO_CHARSET.put( 65, "ASCII");
    INDEX_TO_CHARSET.put( 66, "Cp1250");
    INDEX_TO_CHARSET.put( 67, "Cp1256");
    INDEX_TO_CHARSET.put( 68, "Cp866");
    INDEX_TO_CHARSET.put( 69, "US-ASCII");
    INDEX_TO_CHARSET.put( 70, "GREEK");
    INDEX_TO_CHARSET.put( 71, "HEBREW");
    INDEX_TO_CHARSET.put( 72, "US-ASCII");
    INDEX_TO_CHARSET.put( 73, "Cp895");
    INDEX_TO_CHARSET.put( 74, "KOI8_R");
    INDEX_TO_CHARSET.put( 75, "KOI8_R");
    INDEX_TO_CHARSET.put( 77, "ISO8859_2");
    INDEX_TO_CHARSET.put( 78, "LATIN5");
    INDEX_TO_CHARSET.put( 79, "ISO8859_7");
    INDEX_TO_CHARSET.put( 80, "Cp437");
    INDEX_TO_CHARSET.put( 81, "Cp852");
    INDEX_TO_CHARSET.put( 82, "ISO8859_1");
    INDEX_TO_CHARSET.put( 83, "UTF-8");
    INDEX_TO_CHARSET.put( 84, "Big5");
    INDEX_TO_CHARSET.put( 85, "EUCKR");
    INDEX_TO_CHARSET.put( 86, "GB2312");
    INDEX_TO_CHARSET.put( 87, "GBK");
    INDEX_TO_CHARSET.put( 88, "SJIS");
    INDEX_TO_CHARSET.put( 89, "TIS620");
    INDEX_TO_CHARSET.put( 90, "UTF-16BE");
    INDEX_TO_CHARSET.put( 91, "EUC_JP");
    INDEX_TO_CHARSET.put( 92, "US-ASCII");
    INDEX_TO_CHARSET.put( 93, "US-ASCII");
    INDEX_TO_CHARSET.put( 94, "ISO8859_1");
    INDEX_TO_CHARSET.put( 95, "CP932");
    INDEX_TO_CHARSET.put( 96, "CP932");
    INDEX_TO_CHARSET.put( 97, "EUC_JP_Solaris");
    INDEX_TO_CHARSET.put( 98, "EUC_JP_Solaris");
    INDEX_TO_CHARSET.put( 128, "UTF-16BE");
    INDEX_TO_CHARSET.put( 129, "UTF-16BE");
    INDEX_TO_CHARSET.put( 130, "UTF-16BE");
    INDEX_TO_CHARSET.put( 131, "UTF-16BE");
    INDEX_TO_CHARSET.put( 132, "UTF-16BE");
    INDEX_TO_CHARSET.put( 133, "UTF-16BE");
    INDEX_TO_CHARSET.put( 134, "UTF-16BE");
    INDEX_TO_CHARSET.put( 135, "UTF-16BE");
    INDEX_TO_CHARSET.put( 136, "UTF-16BE");
    INDEX_TO_CHARSET.put( 137, "UTF-16BE");
    INDEX_TO_CHARSET.put( 138, "UTF-16BE");
    INDEX_TO_CHARSET.put( 139, "UTF-16BE");
    INDEX_TO_CHARSET.put( 140, "UTF-16BE");
    INDEX_TO_CHARSET.put( 141, "UTF-16BE");
    INDEX_TO_CHARSET.put( 142, "UTF-16BE");
    INDEX_TO_CHARSET.put( 143, "UTF-16BE");
    INDEX_TO_CHARSET.put( 144, "UTF-16BE");
    INDEX_TO_CHARSET.put( 145, "UTF-16BE");
    INDEX_TO_CHARSET.put( 146, "UTF-16BE");
    INDEX_TO_CHARSET.put( 192, "UTF-8");
    INDEX_TO_CHARSET.put( 193, "UTF-8");
    INDEX_TO_CHARSET.put( 194, "UTF-8");
    INDEX_TO_CHARSET.put( 195, "UTF-8");
    INDEX_TO_CHARSET.put( 196, "UTF-8");
    INDEX_TO_CHARSET.put( 197, "UTF-8");
    INDEX_TO_CHARSET.put( 198, "UTF-8");
    INDEX_TO_CHARSET.put( 199, "UTF-8");
    INDEX_TO_CHARSET.put( 200, "UTF-8");
    INDEX_TO_CHARSET.put( 201, "UTF-8");
    INDEX_TO_CHARSET.put( 202, "UTF-8");
    INDEX_TO_CHARSET.put( 203, "UTF-8");
    INDEX_TO_CHARSET.put( 204, "UTF-8");
    INDEX_TO_CHARSET.put( 205, "UTF-8");
    INDEX_TO_CHARSET.put( 206, "UTF-8");
    INDEX_TO_CHARSET.put( 207, "UTF-8");
    INDEX_TO_CHARSET.put( 208, "UTF-8");
    INDEX_TO_CHARSET.put( 209, "UTF-8");
    INDEX_TO_CHARSET.put( 210, "UTF-8");
    if(root != null) {
      String filePath = Paths.get(root).resolve("index_to_charset.properties").toAbsolutePath()
              .toString();
      Properties prop = new Properties();
      try {
        prop.load(new FileInputStream(filePath));
        for (Object index : prop.keySet()) {
          INDEX_TO_CHARSET.put(Integer.parseInt((String) index), prop.getProperty((String) index));
        }
      } catch (Exception e) {
        LOGGER.error("error:", e);
      }
    }

    // charset --> index
    for (Integer key : INDEX_TO_CHARSET.keySet()) {
      String charset = INDEX_TO_CHARSET.get(key);
      if (charset != null && CHARSET_TO_INDEX.get(charset) == null) {
        CHARSET_TO_INDEX.put(charset, key);
      }
    }
    CHARSET_TO_INDEX.put("iso-8859-1", 14);
    CHARSET_TO_INDEX.put("iso_8859_1", 14);
    CHARSET_TO_INDEX.put("utf-8", 33);
  }

  public static final String getCharset(int index) {
    return INDEX_TO_CHARSET.get(index);
  }

  public static final int getIndex(String charset) {
    if (charset == null || charset.length() == 0) {
      return -1;
    } else {
      Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
      return (i == null) ? -1 : i;
    }
  }


}
