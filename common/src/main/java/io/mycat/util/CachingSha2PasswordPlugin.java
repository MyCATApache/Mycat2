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

import io.mycat.MycatException;
import io.mycat.beans.mysql.ServerVersion;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.DigestException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;

public class CachingSha2PasswordPlugin  {

    private static MycatLogger LOGGER = MycatLoggerFactory
        .getLogger(MysqlNativePasswordPluginUtil.class);
    public static final String PROTOCOL_PLUGIN_NAME = "caching_sha2_password"; // caching_sha2_password

    public static byte[] scrambleCachingSha2(String password, byte[] seed) {
        if (password == null || password.length() == 0) {
            LOGGER.warn("password is empty");
            return new byte[0];
        }
        try {
            return SecurityUtil.scrambleCachingSha2(password.getBytes(), seed);
        } catch (DigestException e) {
            LOGGER.warn("no such Digest", e);
            return null;
        }

    }

    public static byte[] scrambleCachingSha2(String password, String seed) {
        return scrambleCachingSha2(password, seed.getBytes());
    }

    public static byte[] encrypt(String mysqlVersion, String publicKeyString, String password, String seed, String encoding) throws Exception {
        ServerVersion currentVersion = ServerVersion.parseVersion(mysqlVersion);
        final ServerVersion min = new ServerVersion(8, 0, 5);
        if(currentVersion.compareTo(min) >= 0) {
            return encryptPassword("RSA/ECB/OAEPWithSHA-1AndMGF1Padding",  publicKeyString,  password,  seed,  encoding);
        }
        return encryptPassword("RSA/ECB/PKCS1Padding",  publicKeyString,  password,  seed,  encoding);

    }

    public static byte[] encryptPassword(String transformation, String publicKeyString, String password, String seed, String encoding)
        throws Exception {
        byte[] input = null;
        input = password != null ? getBytesNullTerminated(password, encoding) : new byte[] { 0 };
        byte[] mysqlScrambleBuff = new byte[input.length];
        SecurityUtil.xorString(input, mysqlScrambleBuff, seed.getBytes(), input.length);
        return encryptWithRSAPublicKey(mysqlScrambleBuff, decodeRSAPublicKey(publicKeyString), transformation);
    }

    public static byte[] getBytesNullTerminated(String value, String encoding) {
        Charset cs = Charset.forName(encoding);
        ByteBuffer buf = cs.encode(value);
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;

        return asBytes;
    }
    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key, String transformation)
        throws IllegalBlockSizeException, InvalidKeyException, BadPaddingException, NoSuchAlgorithmException, NoSuchPaddingException {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (Exception e) {
            throw e;
        }
    }

    public static RSAPublicKey decodeRSAPublicKey(String key) throws Exception {

        if (key == null) {
            throw new MycatException("Key parameter is null\"");
        }

        int offset = key.indexOf("\n") + 1;
        int len = key.indexOf("-----END PUBLIC KEY-----") - offset;

        // TODO: use standard decoders with Java 6+
        byte[] certificateData = Base64Decoder.decode(key.getBytes(), offset, len);

        X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return (RSAPublicKey) kf.generatePublic(spec);
    }


}
