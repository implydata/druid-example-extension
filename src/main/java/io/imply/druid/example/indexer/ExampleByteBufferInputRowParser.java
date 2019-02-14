/*
 * Copyright 2019 Imply Data, Inc.
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

package io.imply.druid.example.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class ExampleByteBufferInputRowParser implements ByteBufferInputRowParser
{
  public static final String TYPE_NAME = "exampleParser";

  private final ParseSpec parseSpec;
  private StringInputRowParser stringParser;

  private final Base64.Decoder base64Decoder = Base64.getDecoder();

  @JsonCreator
  public ExampleByteBufferInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ExampleByteBufferInputRowParser(parseSpec);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    if (stringParser == null) {
      stringParser = new StringInputRowParser(parseSpec);
    }
    String stringInput = decodeRot13Base64(input);
    return ImmutableList.of(stringParser.parse(stringInput));
  }

  public String decodeRot13Base64(ByteBuffer input)
  {
    String rot13encodedBase64 = StringUtils.fromUtf8(input);
    String base64 = rot13(rot13encodedBase64);
    return StringUtils.fromUtf8(base64Decoder.decode(base64));
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
    ExampleByteBufferInputRowParser that = (ExampleByteBufferInputRowParser) o;
    return parseSpec.equals(that.parseSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec);
  }

  @Override
  public String toString()
  {
    return "ExampleByteBufferInputRowParser{" +
           "parseSpec=" + parseSpec +
           '}';
  }

  @VisibleForTesting
  static String rot13(String rot13String)
  {
    char[] rotated = new char[rot13String.length()];
    for (int i = 0; i < rot13String.length(); i++) {
      char c = rot13String.charAt(i);
      if (c >= 'a' && c <= 'm') {
        c += 13;
      } else if (c >= 'A' && c <= 'M') {
        c += 13;
      } else if (c >= 'n' && c <= 'z') {
        c -= 13;
      } else if (c >= 'N' && c <= 'Z') {
        c -= 13;
      }
      rotated[i] = c;
    }
    return new String(rotated);
  }
}
