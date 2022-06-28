package io.imply.druid.example.aggregator;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.example.ExampleExtensionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class ExampleSumAggregatorFactoryTest
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
    final ExampleSumAggregatorFactory agg = new ExampleSumAggregatorFactory("billy", "nilly");
    Assert.assertEquals("billy", agg.getName());
    Assert.assertEquals("nilly", agg.getFieldName());
    Assert.assertEquals(ColumnType.FLOAT, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.FLOAT, agg.getResultType());
    Assert.assertEquals(357.2, agg.combine(123.2, 234));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ExampleSumAggregatorFactory agg = new ExampleSumAggregatorFactory("billy", "nilly");

    Assert.assertEquals(
        new ExampleSumAggregatorFactory("billy", "nilly"),
        MAPPER.readValue("{ \"type\" : \"exampleSum\", \"name\" : \"billy\",  \"fieldName\": \"nilly\"}", ExampleSumAggregatorFactory.class)
    );

    Assert.assertEquals(
        new ExampleSumAggregatorFactory("billy", "nilly"),
        MAPPER.readValue(MAPPER.writeValueAsBytes(agg), ExampleSumAggregatorFactory.class)
    );
  }

}
