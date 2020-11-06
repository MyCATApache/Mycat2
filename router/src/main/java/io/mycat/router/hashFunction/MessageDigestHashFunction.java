package io.mycat.router.hashFunction;

import java.security.MessageDigest;

public class MessageDigestHashFunction implements HashFunction {
    MessageDigest instance;

    public MessageDigestHashFunction(MessageDigest digest) {
        instance = digest;
    }

    @Override
    public long hash(byte[] bytes) {
        instance.reset();
        instance.update(bytes);
        byte[] digest = instance.digest();
        long h = 0;
        int length = digest.length;
        for (int i = 0; i < length; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h;
    }
}