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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.ExtractionFn;
import io.imply.druid.example.ExampleExtensionModule;
import org.junit.Assert;
import org.junit.Test;

public class ExampleExtractionFnTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    for (Module module : new ExampleExtensionModule().getJacksonModules()) {
      MAPPER.registerModule(module);
    }
  }

  @Test
  public void testSimple()
  {
    final ExampleExtractionFn fn = new ExampleExtractionFn(3);
    Assert.assertEquals(null, fn.apply(null));
    Assert.assertEquals("x", fn.apply("x"));
    Assert.assertEquals("foo", fn.apply("foo"));
    Assert.assertEquals("foo", fn.apply("foobar"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ExampleExtractionFn fn = new ExampleExtractionFn(3);

    Assert.assertEquals(
        new ExampleExtractionFn(3),
        MAPPER.readValue("{ \"type\" : \"example\", \"length\" : 3 }", ExtractionFn.class)
    );

    Assert.assertEquals(
        new ExampleExtractionFn(3),
        MAPPER.readValue(MAPPER.writeValueAsBytes(fn), ExtractionFn.class)
    );
  }
}
