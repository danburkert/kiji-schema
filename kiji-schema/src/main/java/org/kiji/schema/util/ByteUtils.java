/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.util;

import java.nio.ByteBuffer;

import com.google.common.base.Charsets;

/**
 * Useful static methods for working with byte arrays.
 *
 * <p>Byte arrays should not be modified once provided to any method in this class, as defensive
 * copies are not guaranteed.</p>
 *
 * <p></p>
 *
 */
public final class ByteUtils {

  /**
   * Convert a UTF-8 encoded byte array to a {@link String}. The conversion between {@link String}s
   * and byte arrays is not 1 to 1, so this method is *not* safe to use on arbitrary inputs.
   * This method should only be used with byte arrays containing UTF-8 encoded {@code strings}, such
   * as those returned from {@link #toBytes(String)}.
   *
   * If the provided byte array is {@code null}, this method will return {@code null}.
   *
   * @return the String equivalent of the provided UTF-8 encoded bytes.
   */
  public static String toString(byte[] bytes) {
    return bytes == null ? null : new String(bytes, Charsets.UTF_8);
  }

  /**
   * Convert a {@link String} into a UTF-8 encoded byte array.
   *
   * If the provided {@link String} is {@code null}, this method will return {@code null}.
   *
   * @param string to convert to a bytes.
   * @return a byte array containing the UTF-8 encoded bytes of the string.
   */
  public static byte[] toBytes(String string) {
    return string == null ? null : string.getBytes(Charsets.UTF_8);
  }

  /**
   * Convert a {@link ByteBuffer} to a byte array.
   *
   * @param buffer to convert to a byte array.
   * @return the converted byte array.
   */
  public static byte[] toBytes(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    if (buffer.hasArray() && buffer.position() == 0 && buffer.capacity() == buffer.limit()) {
      return buffer.array();
    }

    int position = buffer.position();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    buffer.position(position);
    return bytes;
  }

  /**
   * Private constructor.
   */
  private ByteUtils() {}
}
