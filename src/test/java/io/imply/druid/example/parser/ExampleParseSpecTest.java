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

package io.imply.druid.example.parser;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.SubstringDimExtractionFn;
import io.imply.druid.example.ExampleExtensionModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class ExampleParseSpecTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final ParseSpec PARSE_SPEC = new ExampleParseSpec(
      new TimestampSpec("timestamp", "auto", null),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("foo", "bar")),
          null,
          null
      ),
      new SubstringDimExtractionFn(0, 1)
  );

  static {
    for (Module module : new ExampleExtensionModule().getJacksonModules()) {
      MAPPER.registerModule(module);
    }
  }

  @Test
  public void testParser() throws Exception
  {
    final Parser<String, Object> parser = PARSE_SPEC.makeParser();
    final Map<String, Object> parsed = parser.parse(
        "{ \"timestamp\" : \"2000\", \"foo\" : \"bar\", \"baz\" : 40, \"qux\" : [\"abc\", \"def\"] }"
    );
    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("timestamp", "2000")
            .put("foo", "b")
            .put("baz", 40)
            .put("qux", ImmutableList.of("a", "d"))
            .build(),
        parsed
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    Assert.assertEquals(
        PARSE_SPEC,
        MAPPER.readValue(
            "{\n"
            + "  \"format\": \"example\",\n"
            + "  \"timestampSpec\" : { \"timestampColumn\" : \"timestamp\", \"timestampFormat\" : \"auto\" },\n"
            + "  \"dimensionsSpec\": { \"dimensions\" : [\"foo\", \"bar\"] },\n"
            + "  \"extractionFn\" : { \"type\" : \"substring\", \"index\" : 0, \"length\" : 1 }\n"
            + "}",
            ParseSpec.class
        )
    );

    Assert.assertEquals(
        PARSE_SPEC,
        MAPPER.readValue(MAPPER.writeValueAsBytes(PARSE_SPEC), ParseSpec.class)
    );
  }
}
