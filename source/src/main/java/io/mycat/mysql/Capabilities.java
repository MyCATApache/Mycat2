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
package io.mycat.mysql;

/**
 * 处理能力标识定义
 * 
 * @author mycat cjw
 */
public interface Capabilities {

    /**
     * server value
     * 
     * <pre>
     * server:        11110111 11111111
     * client_cmd: 11 10100110 10000101
     * client_jdbc:10 10100010 10001111
     *  
     * @see http://dev.mysql.com/doc/refman/5.1/en/mysql-real-connect.html
     * </pre>
     */
    // new more secure passwords
    public static final int CLIENT_LONG_PASSWORD = 1;

    // Found instead of affected rows
    // 返回找到（匹配）的行数，而不是改变了的行数。
    public static final int CLIENT_FOUND_ROWS = 2;

    // Get all column flags
    public static final int CLIENT_LONG_FLAG = 4;

    // One can specify db on connect
    public static final int CLIENT_CONNECT_WITH_DB = 8;

    // Don't allow database.table.column
    // 不允许“数据库名.表名.列名”这样的语法。这是对于ODBC的设置。
    // 当使用这样的语法时解析器会产生一个错误，这对于一些ODBC的程序限制bug来说是有用的。
    public static final int CLIENT_NO_SCHEMA = 16;

    // Can use compression protocol
    // 使用压缩协议
    public static final int CLIENT_COMPRESS = 32;

    // Odbc client
    public static final int CLIENT_ODBC = 64;

    // Can use LOAD DATA LOCAL
    public static final int CLIENT_LOCAL_FILES = 128;

    // Ignore spaces before '('
    // 允许在函数名后使用空格。所有函数名可以预留字。
    public static final int CLIENT_IGNORE_SPACE = 256;

    // New 4.1 protocol This is an interactive client
    public static final int CLIENT_PROTOCOL_41 = 512;

    // This is an interactive client
    // 允许使用关闭连接之前的不活动交互超时的描述，而不是等待超时秒数。
    // 客户端的会话等待超时变量变为交互超时变量。
    public static final int CLIENT_INTERACTIVE = 1024;

    // Switch to SSL after handshake
    // 使用SSL。这个设置不应该被应用程序设置，他应该是在客户端库内部是设置的。
    // 可以在调用mysql_real_connect()之前调用mysql_ssl_set()来代替设置。
    public static final int CLIENT_SSL = 2048;

    // IGNORE sigpipes
    // 阻止客户端库安装一个SIGPIPE信号处理器。
    // 这个可以用于当应用程序已经安装该处理器的时候避免与其发生冲突。
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;

    // Client knows about transactions
    public static final int CLIENT_TRANSACTIONS = 8192;

    // Old flag for 4.1 protocol
    public static final int CLIENT_RESERVED = 16384;

    // New 4.1 authentication
    public static final int CLIENT_SECURE_CONNECTION = 32768;

    // Enable/disable multi-stmt support
    // 通知服务器客户端可以发送多条语句（由分号分隔）。如果该标志为没有被设置，多条语句执行。
    public static final int CLIENT_MULTI_STATEMENTS = 1<<16;

    // Enable/disable multi-results
    // 通知服务器客户端可以处理由多语句或者存储过程执行生成的多结果集。
    // 当打开CLIENT_MULTI_STATEMENTS时，这个标志自动的被打开。
    public static final int CLIENT_MULTI_RESULTS = 1<<1<<16;

    /**
     ServerCan send multiple resultsets for COM_STMT_EXECUTE.
     Client
     Can handle multiple resultsets for COM_STMT_EXECUTE.
     Value
     0x00040000
     Requires
     CLIENT_PROTOCOL_41
     */
    public static final int  CLIENT_PS_MULTI_RESULTS = 1<<2<<16;
    /**
     Server
     Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.

     Client
     Supports authentication plugins.

     Requires
     CLIENT_PROTOCOL_41
     */
    public static final int CLIENT_PLUGIN_AUTH = 1<<3<<16;

    /**
     Value
     0x00100000

     Server
     Permits connection attributes in Protocol::HandshakeResponse41.

     Client
     Sends connection attributes in Protocol::HandshakeResponse41.
     */

    public static final int CLIENT_CONNECT_ATTRS = 1<<4<<16;

    /**
     Value
     0x00200000

     Server
     Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.

     Client
     Length of auth response data in Protocol::HandshakeResponse41 is a length-encoded integer.
     The flag was introduced in 5.6.6, but had the wrong value.
     */
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1<<5<<16;

    /**
     *Value
     * 0x00400000
     *
     * Server
     * Announces support for expired password extension.
     *
     * Client
     * Can handle expired passwords.
     */
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1<<6<<16;

    /**
     * Value
     * 0x00800000
     *
     * Server
     * Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.
     *
     * Client
     * Expects the server to send sesson-state changes after a OK packet.
     */
    public static final int CLIENT_SESSION_TRACK = 1<<7<<16;

    /**
     Value
     0x01000000

     Server
     Can send OK after a Text Resultset.

     Client
     Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.

     Background
     To support CLIENT_SESSION_TRACK, additional information must be sent after all successful commands. Although the OK packet is extensible, the EOF packet is not due to the overlap of its bytes with the content of the Text Resultset Row.

     Therefore, the EOF packet in the Text Resultset is replaced with an OK packet. EOF packets are deprecated as of MySQL 5.7.5.
     */
    public static final int CLIENT_DEPRECATE_EOF = 1<<8<<16;
}