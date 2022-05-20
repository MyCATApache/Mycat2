package io.mycat.util;

import java.util.Base64;

import java.io.UnsupportedEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 封装Java Util包Base64方法 */
public class Base64Utils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Base64Utils.class);

  public static final String encodingUTF_8 = "UTF-8";
  public static Base64.Encoder encoder;
  public static Base64.Decoder decoder;

  static {
    decoder = Base64.getDecoder();
    encoder = Base64.getEncoder();
  }

  public static byte[] encodeBase64(byte[] bytes) {
    return encoder.encode(bytes);
  }

  public static String encodeBase64(String source) {
    byte[] bytes = encodeBase64(source.getBytes());
    try {
      return new String(bytes, encodingUTF_8);
    } catch (UnsupportedEncodingException ex) {
      LOGGER.error(ex.getMessage(), ex);
    }
    return null;
  }

  public static String encodeBase64String(byte[] bytes) {
    return encoder.encodeToString(bytes);
  }

  public static byte[] encodeBase64Byte(String source) {
    byte[] bytes = encodeBase64(source.getBytes());
    return bytes;
  }

  public static byte[] decodeBase64(byte[] bytes) {
    return decoder.decode(bytes);
  }

  public static byte[] decodeBase64Byte(String string) {
    return decoder.decode(string.getBytes());
  }

  public static String decodeBase64String(byte[] bytes) {
    try {
      return new String(decoder.decode(bytes), encodingUTF_8);
    } catch (UnsupportedEncodingException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return null;
  }

  public static String decodeBase64(String string) {
    byte[] decode = decodeBase64(string.getBytes());
    try {
      return new String(decode, encodingUTF_8);
    } catch (UnsupportedEncodingException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return null;
  }
}
