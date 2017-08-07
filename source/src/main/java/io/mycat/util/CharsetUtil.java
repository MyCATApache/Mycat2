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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该类被彻底重构，fix 掉了原来的 collationIndex 和 charset 之间对应关系的兼容性问题， 比如 utf8mb4 对应的
 * collationIndex 有 45, 46 两个值，如果我们只配置一个 45或者46的话，
 * 那么当mysqld(my.cnf配置文件)中的配置了：collation_server=utf8mb4_bin时，而我们却仅仅
 * 值配置45的话，那么就会报错：'java.lang.RuntimeException: Unknown charsetIndex:46' 如果没有配置
 * collation_server=utf8mb4_bin，那么collation_server就是使用的默认值，而我们却仅仅
 * 仅仅配置46，那么也会报错。所以应该要同时配置45,46两个值才是正确的。 重构方法是，在 MycatServer.startup()方法在中，在
 * config.initDatasource(); 之前，加入
 * CharsetUtil.initCharsetAndCollation(config.getDataHosts());
 * 该方法，直接从mysqld的information_schema.collations表中获取 collationIndex 和 charset
 * 之间对应关系， 因为是从mysqld服务器获取的，所以肯定不会出现以前的兼容性问题(不同版本的mysqld，collationIndex 和
 * charset 对应关系不一样)。
 * 
 * @author mycat
 */
public class CharsetUtil {
    public static final Logger logger = LoggerFactory.getLogger(CharsetUtil.class);

    /** collationIndex 和 charsetName 的映射 */
    private static final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();

    /** charsetName 到 默认collationIndex 的映射 */
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

    /** collationName 到 CharsetCollation 对象的映射 */
    @SuppressWarnings("unused")
	private static final Map<String, CharsetCollation> COLLATION_TO_CHARSETCOLLATION = new HashMap<>();

    public static final String getCharset(int index) {
        String charset= INDEX_TO_CHARSET.get(index);
        if(charset==null)
        {
        	//System.out.println("warning charset is null ,not loaded from server ,please fix it !!");
        	logger.warn("charset is null ,not loaded from server ,please fix it !!");
        	return "UTF-8";
        }
        return charset;
    }

    /**
     * 因为 每一个 charset 对应多个 collationIndex, 所以这里返回的是默认的那个 collationIndex；
     * 如果想获得确定的值 index，而非默认的index, 那么需要使用 getIndexByCollationName 或者
     * getIndexByCharsetNameAndCollationName
     * 
     * @param charset
     * @return
     */
    public static final int getIndex(String charset) {
        if (charset == null || charset.trim().equals("")) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            if (i == null && "Cp1252".equalsIgnoreCase(charset))
                charset = "latin1"; // 参见：http://www.cp1252.com/ The windows
                                    // 1252 codepage, also called Latin 1

            i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i;
        }
    }

}

/**
 * 该类用来表示 mysqld 数据库中 字符集、字符集支持的collation、字符集的collation的index、 字符集的默认collation
 * 的对应关系： 一个字符集一般对应(支持)多个collation，其中一个是默认的 collation，每一个 collation对应一个唯一的index,
 * collationName 和 collationIndex 一一对应，
 * 每一个collationIndex对应到一个字符集，不同的collationIndex可以对应到相同的字符集， 所以字符集 到
 * collationIndex 的对应不是唯一的，一个字符集对应多个 index(有一个默认的 collation的index)， 而
 * collationIndex 到 字符集 的对应是确定的，唯一的； mysqld 用 collation 的 index 来描述排序规则。
 * 
 * @author Administrator
 *
 */
class CharsetCollation {
    // mysqld支持的字符编码名称，注意这里不是java中的unicode编码的名字，
    // 二者之间的区别和联系可以参考驱动jar包中的com.mysql.jdbc.CharsetMapping源码
    private String charsetName;
    private int collationIndex; // collation的索引顺序
    private String collationName; // collation 名称
    private boolean isDefaultCollation = false; // 该collation是否是字符集的默认collation

    public CharsetCollation(String charsetName, int collationIndex, String collationName, boolean isDefaultCollation) {
        this.charsetName = charsetName;
        this.collationIndex = collationIndex;
        this.collationName = collationName;
        this.isDefaultCollation = isDefaultCollation;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public int getCollationIndex() {
        return collationIndex;
    }

    public void setCollationIndex(int collationIndex) {
        this.collationIndex = collationIndex;
    }

    public String getCollationName() {
        return collationName;
    }

    public void setCollationName(String collationName) {
        this.collationName = collationName;
    }

    public boolean isDefaultCollation() {
        return isDefaultCollation;
    }

    public void setDefaultCollation(boolean isDefaultCollation) {
        this.isDefaultCollation = isDefaultCollation;
    }
}
