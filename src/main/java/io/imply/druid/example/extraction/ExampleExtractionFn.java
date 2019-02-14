/*
 * Copyright 2016 Imply Data, Inc.
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

package io.imply.druid.example.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import org.apache.druid.query.extraction.DimExtractionFn;

import java.nio.ByteBuffer;

/**
 * ExtractionFn that returns the first "length" characters of a string.
 */
public class ExampleExtractionFn extends DimExtractionFn
{
  public static final String TYPE_NAME = "example";

  // Built-in extractionFns use 1-byte codes starting with 0x00 for cache keys.
  // Extension extractionFns can use 0xFF + our own site-specific byte.
  private static final byte[] CACHE_KEY_PREFIX = new byte[]{(byte) 0xFF, (byte) 0x00};

  private final int length;

  @JsonCreator
  public ExampleExtractionFn(
      @JsonProperty("length") int length
  )
  {
    this.length = length;
  }

  @JsonProperty
  public int getLength()
  {
    return length;
  }

  @Override
  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(CACHE_KEY_PREFIX.length + Ints.BYTES)
                     .put(CACHE_KEY_PREFIX)
                     .putInt(length)
                     .array();
  }

  @Override
  public String apply(final String value)
  {
    if (value == null) {
      return null;
    } else if (value.length() <= length) {
      return value;
    } else {
      return value.substring(0, length);
    }
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExampleExtractionFn that = (ExampleExtractionFn) o;

    return length == that.length;

  }

  @Override
  public int hashCode()
  {
    return length;
  }

  @Override
  public String toString()
  {
    return "ExampleExtractionFn{" +
           "length=" + length +
           '}';
  }
}
