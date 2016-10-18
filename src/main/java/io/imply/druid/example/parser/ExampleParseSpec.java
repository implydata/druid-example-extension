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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.extraction.ExtractionFn;

import java.util.List;
import java.util.Map;

/**
 * ParseSpec that parses inputs as JSON and applies an extractionFn to every string value except the timestamp.
 */
public class ExampleParseSpec extends ParseSpec
{
  public static final String TYPE_NAME = "example";

  private final ExtractionFn extractionFn;
  private final ObjectMapper objectMapper;

  @JsonCreator
  public ExampleParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    super(timestampSpec, dimensionsSpec);
    this.extractionFn = Preconditions.checkNotNull(extractionFn, "extractionFn");
    this.objectMapper = new ObjectMapper();
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new Parser<String, Object>()
    {
      @Override
      public Map<String, Object> parse(final String input)
      {
        final Map<String, Object> object;

        try {
          object = objectMapper.readValue(
              input,
              new TypeReference<Map<String, Object>>()
              {
              }
          );
        }
        catch (Exception e) {
          throw new ParseException(e, "Unable to parse row [%s]", input);
        }

        final Map<String, Object> newObject = Maps.newLinkedHashMap();
        for (Map.Entry<String, Object> entry : object.entrySet()) {
          final Object value = entry.getKey().equals(getTimestampSpec().getTimestampColumn())
                               ? entry.getValue()
                               : transformValue(entry.getValue());
          newObject.put(entry.getKey(), value);
        }

        return newObject;
      }

      @Override
      public void setFieldNames(final Iterable<String> fieldNames)
      {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public List<String> getFieldNames()
      {
        throw new UnsupportedOperationException("not supported");
      }
    };
  }

  @Override
  public ParseSpec withTimestampSpec(final TimestampSpec spec)
  {
    return new ExampleParseSpec(spec, getDimensionsSpec(), extractionFn);
  }

  @Override
  public ParseSpec withDimensionsSpec(final DimensionsSpec spec)
  {
    return new ExampleParseSpec(getTimestampSpec(), spec, extractionFn);
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
    if (!super.equals(o)) {
      return false;
    }

    ExampleParseSpec that = (ExampleParseSpec) o;

    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  private Object transformValue(final Object value)
  {
    if (value instanceof String) {
      return extractionFn.apply((String) value);
    } else if (value instanceof List) {
      return ImmutableList.copyOf(
          Lists.transform(
              (List) value,
              new Function()
              {
                @Override
                public Object apply(Object o)
                {
                  return transformValue(o);
                }
              }
          )
      );
    } else {
      return value;
    }
  }

}
