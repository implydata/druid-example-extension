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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.imply.druid.example.ExampleExtensionModule;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ExampleByteBufferInputRowParserTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    for (Module module : new ExampleExtensionModule().getJacksonModules()) {
      MAPPER.registerModule(module);
    }
  }

  final String[] inputRows = new String[]{
      "2011-04-15T00:00:00.000Z	spot	automotive	preferred	apreferred	106.793700",
      "2011-04-15T00:00:00.000Z	spot	business	preferred	bpreferred	94.469747",
      "2011-04-15T00:00:00.000Z	spot	entertainment	preferred	epreferred	135.109191",
      "2011-04-15T00:00:00.000Z	spot	health	preferred	hpreferred	99.596909",
      "2011-04-15T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	92.782760",
      "2011-04-15T00:00:00.000Z	spot	news	preferred	npreferred",
      "2011-04-15T00:00:00.000Z	spot	premium	preferred	ppreferred",
      "2011-04-15T00:00:00.000Z	spot	technology	preferred	tpreferred",
      "2011-04-15T00:00:00.000Z	spot	travel	preferred	tpreferred",
      "2011-04-15T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred",
      "2011-04-15T00:00:00.000Z	total_market	premium	preferred	ppreferred",
      "2011-04-15T00:00:00.000Z	upfront	mezzanine	preferred	mpreferred",
      "2011-04-15T00:00:00.000Z	upfront	premium	preferred	ppreferred"
  };
  String metricSpec = "[{ \"type\": \"count\", \"name\": \"count\"}]";

  String parseSpecJson =
      "{"
      + "  \"format\" : \"tsv\","
      + "  \"timestampSpec\" : {"
      + "      \"column\" : \"timestamp\","
      + "      \"format\" : \"auto\""
      + "  },"
      + "  \"dimensionsSpec\" : {"
      + "      \"dimensions\": [],"
      + "      \"dimensionExclusions\" : [],"
      + "      \"spatialDimensions\" : []"
      + "  },"
      + "  \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
      + "}";

  ParseSpec parseSpec;
  ExampleByteBufferInputRowParser parser;

  Queue<ByteBuffer> inputRowBuffers = new LinkedList<>();

  @Before
  public void setup() throws IOException
  {
    NullHandling.initializeForTests();
    parseSpec = MAPPER.readValue(parseSpecJson, ParseSpec.class);
    parser = new ExampleByteBufferInputRowParser(parseSpec);

    for (String row : inputRows) {
      String base64 = Base64.getEncoder().encodeToString(StringUtils.toUtf8(row));
      String rot13Base64 = ExampleByteBufferInputRowParser.rot13(base64);
      inputRowBuffers.offer(ByteBuffer.wrap(StringUtils.toUtf8(rot13Base64)));
    }
  }


  @Test
  public void testSerde() throws IOException
  {
    Assert.assertEquals(
        parser,
        MAPPER.readValue(MAPPER.writeValueAsString(parser), ExampleByteBufferInputRowParser.class)
    );
  }

  @Test
  public void testParse()
  {
    int i = 0;
    StringInputRowParser stringParser = new StringInputRowParser(parseSpec);

    while(!inputRowBuffers.isEmpty()) {
      ByteBuffer row = inputRowBuffers.poll();
      List<InputRow> parsed = parser.parseBatch(row);

      if (i == 0) {
        InputRow theRow = parsed.get(0);
        Assert.assertEquals(new DateTime("2011-04-15T00:00:00.000Z"), theRow.getTimestamp());
        Assert.assertEquals("spot", theRow.getDimension("market").get(0));
        Assert.assertEquals("automotive", theRow.getDimension("quality").get(0));
        Assert.assertEquals("preferred", theRow.getDimension("placement").get(0));
        Assert.assertEquals(ImmutableList.of("a", "preferred"), theRow.getDimension("placementish"));
        Assert.assertEquals("106.793700", theRow.getDimension("index").get(0));
      }

      Assert.assertEquals(ImmutableList.of(stringParser.parse(inputRows[i++])), parsed);
    }
  }
}
