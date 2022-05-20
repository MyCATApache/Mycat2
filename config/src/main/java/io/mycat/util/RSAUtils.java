package io.mycat.util;


import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import lombok.extern.slf4j.Slf4j;
import okio.ByteString;

@Slf4j
public class RSAUtils {
  public static void main(String[] args) throws Exception {

    System.out.println(ByteString.encodeUtf8("zhangsan").base64());
    System.out.println(decodeBase64("emhhbmdzYW4="));
    // 生成公钥和私钥
    Map<Integer, String> keyMap = genKeyPair();
    // 加密字符串
    Long time = System.currentTimeMillis();
    String message = "1234567";
    System.out.println("随机生成的公钥为:" + keyMap.get(0));
    System.out.println("随机生成的私钥为:" + keyMap.get(1));
    String messageEn = encrypt(message, keyMap.get(0));
    System.out.println(message + "\n加密后的字符串为:" + messageEn);
    String messageDe = decrypt(messageEn, keyMap.get(1));
    System.out.println("还原后的字符串为:" + messageDe);

    System.out.println("===========");
    String messageEn1 = privateKeyEncrypt(message, keyMap.get(1));
    System.out.println(message + "\n加密后的字符串为:" + messageEn1);
    String messageDe1 = publicKeyDecrypt(messageEn1, keyMap.get(0));
    System.out.println("还原后的字符串为:" + messageDe1);
  }

  /**
   * 随机生成密钥对
   *
   * @throws NoSuchAlgorithmException
   */
  public static Map<Integer, String> genKeyPair()
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    // KeyPairGenerator类用于生成公钥和私钥对，基于RSA算法生成对象
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
    // 初始化密钥对生成器，密钥大小为96-1024位
    keyPairGen.initialize(1024, new SecureRandom());
    // 生成一个密钥对，保存在keyPair中
    KeyPair keyPair = keyPairGen.generateKeyPair();
    RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate(); // 得到私钥
    RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic(); // 得到公钥
    String publicKeyString = encodeBase64(publicKey.getEncoded());
    // 得到私钥字符串
    String privateKeyString = encodeBase64((privateKey.getEncoded()));
    Map<Integer, String> keyMap = new HashMap<>(); // 用于封装随机产生的公钥与私钥
    // 将公钥和私钥保存到Map
    keyMap.put(0, publicKeyString); // 0表示公钥
    keyMap.put(1, privateKeyString); // 1表示私钥
    return keyMap;
  }
  /**
   * RSA公钥加密
   *
   * @param str 加密字符串
   * @param publicKey 公钥
   * @return 密文
   * @throws Exception 加密过程中的异常信息
   */
  public static String encrypt(String str, String publicKey) throws Exception {
    // base64编码的公钥
    byte[] decoded = decodeBase64ToBytes(publicKey);
    RSAPublicKey pubKey =
        (RSAPublicKey)
            KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(decoded));
    // RSA加密
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.ENCRYPT_MODE, pubKey);
    String outStr = encodeBase64(cipher.doFinal(str.getBytes("UTF-8")));
    return outStr;
  }

  /**
   * RSA私钥解密
   *
   * @param str 加密字符串
   * @param privateKey 私钥
   * @return 铭文
   * @throws Exception 解密过程中的异常信息
   */
  public static String decrypt(String str, String privateKey) throws Exception {
    // 64位解码加密后的字符串
    byte[] inputByte = decodeBase64ToBytes(str);
    // base64编码的私钥
    byte[] decoded = decodeBase64ToBytes(privateKey);
    RSAPrivateKey priKey =
        (RSAPrivateKey)
            KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(decoded));
    // RSA解密
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.DECRYPT_MODE, priKey);
    String outStr = new String(cipher.doFinal(inputByte));
    return outStr;
  }

  /**
   * RSA私钥加密
   *
   * @param str
   * @param privateKey
   * @return
   * @throws Exception
   */
  public static String privateKeyEncrypt(String str, String privateKey) throws Exception {
    // base64编码的公钥
    byte[] decoded = decodeBase64ToBytes(privateKey);
    PrivateKey priKey =
        KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(decoded));
    // RSA加密
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.ENCRYPT_MODE, priKey);
    String outStr = encodeBase64(cipher.doFinal(str.getBytes()));
    return outStr;
  }

  /**
   * RSA公钥解密
   *
   * @param str
   * @param publicKey
   * @return
   * @throws Exception
   */
  public static String publicKeyDecrypt(String str, String publicKey) throws Exception {
    // 64位解码加密后的字符串
    byte[] inputByte = decodeBase64ToBytes(str);
    // base64编码的私钥
    byte[] decoded = decodeBase64ToBytes(publicKey);
    PublicKey pubKey =
        KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(decoded));
    // RSA解密
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.DECRYPT_MODE, pubKey);
    String outStr = new String(cipher.doFinal(inputByte));
    return outStr;
  }

  public static String encodeBase64(byte[] data)  {
    return Base64Utils.encodeBase64String(data);
  }

  public static String encodeBase64(String data) {
    return Base64Utils.encodeBase64(data);
  }

  public static String decodeBase64(String data) {
    return Base64Utils.decodeBase64(data);
  }
  public static byte[] decodeBase64ToBytes(String data)  {
    return Base64Utils.decodeBase64Byte(data);
  }
}
