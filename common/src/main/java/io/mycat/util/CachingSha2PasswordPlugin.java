/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package io.mycat.util;

import java.security.DigestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingSha2PasswordPlugin  {
    private static Logger logger = LoggerFactory.getLogger(MysqlNativePasswordPluginUtil.class);
    public static final String PROTOCOL_PLUGIN_NAME = "caching_sha2_password"; // caching_sha2_password
    public static void print(byte[] ans) {
        for(byte b : ans){
            System.out.printf("%x ",b);
        }
        System.out.printf("=======================\n");
    }
    public static byte[] scrambleCachingSha2(String password, byte[] seed) {
        if (password == null || password.length() == 0) {
            logger.warn("password is empty");
            return new byte[0];
        }
        print(password.getBytes());
        print(seed);
        try {
            return SecurityUtil.scrambleCachingSha2(password.getBytes(), seed);
        } catch (DigestException e) {
            logger.warn("no such Digest", e);
            return null;
        }

    }

    public static byte[] scrambleCachingSha2(String password, String seed) {
        return scrambleCachingSha2(password, seed.getBytes());
    }

}
