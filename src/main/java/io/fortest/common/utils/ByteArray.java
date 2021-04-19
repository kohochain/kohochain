

package io.fortest.common.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;

@Slf4j(topic = "utils")
public class ByteArray {

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  public static String toHexString(byte[] data) {
    return data == null ? "" : Hex.toHexString(data);
  }

  /**
   * get bytes data from hex string data.
   */
  public static byte[] fromHexString(String data) {
    if (data == null) {
      return EMPTY_BYTE_ARRAY;
    }
    if (data.startsWith("0x")) {
      data = data.substring(2);
    }
    if (data.length() % 2 != 0) {
      data = "0" + data;
    }
    return Hex.decode(data);
  }

  /**
   * get long data from bytes data.
   */
  public static long toLong(byte[] b) {
    return ArrayUtils.isEmpty(b) ? 0 : new BigInteger(1, b).longValue();
  }

  /**
   * get int data from bytes data.
   */
  public static int toInt(byte[] b) {
    return ArrayUtils.isEmpty(b) ? 0 : new BigInteger(1, b).intValue();
  }

  /**
   * get bytes data from string data.
   */
  public static byte[] fromString(String s) {
    return StringUtils.isBlank(s) ? null : s.getBytes();
  }

  /**
   * get string data from bytes data.
   */
  public static String toStr(byte[] b) {
    return ArrayUtils.isEmpty(b) ? null : new String(b);
  }

  public static byte[] fromLong(long val) {
    return Longs.toByteArray(val);
  }

  public static byte[] fromInt(int val) {
    return Ints.toByteArray(val);
  }

  /**
   * get bytes data from object data.
   */
  public static byte[] fromObject(Object obj) {
    byte[] bytes = null;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(obj);
      objectOutputStream.flush();
      bytes = byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      logger.error("objectToByteArray failed: " + e.getMessage(), e);
    }
    return bytes;
  }

  /**
   * Generate a subarray of a given byte array.
   *
   * @param input the input byte array
   * @param start the start index
   * @param end the end index
   * @return a subarray of <tt>input</tt>, ranging from <tt>start</tt> (inclusively) to <tt>end</tt>
   * (exclusively)
   */
  public static byte[] subArray(byte[] input, int start, int end) {
    byte[] result = new byte[end - start];
    System.arraycopy(input, start, result, 0, end - start);
    return result;
  }

  public static boolean matrixContains(List<byte[]> source, byte[] obj) {
    for (byte[] sobj : source) {
      if (Arrays.equals(sobj, obj)) {
        return true;
      }
    }
    return false;
  }
}
