package io.mycat.router.hashFunction;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5MessageDigest  extends MessageDigestHashFunction{
    public MD5MessageDigest() throws NoSuchAlgorithmException {
        super(MessageDigest.getInstance("MD5"));
    }
}