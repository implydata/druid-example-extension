package io.imply.druid.example.aggregator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ExampleSumAggregatorTest
{
  private ExampleSumAggregatorFactory exampleSumAggFactory;
  private ExampleSumAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestFloatColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private float[] floats = {1.1897f, 0.001f, 86.23f, 166.228f};
  private Float[] objects = {2.1897f, 1.001f, 87.23f, 167.228f};

  @Before
  public void setup()
  {
    exampleSumAggFactory = new ExampleSumAggregatorFactory("billy", "nilly");
    combiningAggFactory = (ExampleSumAggregatorFactory) exampleSumAggFactory.getCombiningFactory();
    valueSelector = new TestFloatColumnSelector(floats);
    objectSelector = new TestObjectColumnSelector<>(objects);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testExampleSumAggregator()
  {
    Aggregator agg = exampleSumAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Double result = (Double) agg.get();

    Assert.assertEquals(253.6487, result, 0.0001);
    Assert.assertEquals(253L, agg.getLong());
    Assert.assertEquals(253.6487, agg.getFloat(), 0.0001);
  }

  @Test
  public void testExampleSumBufferAggregator()
  {
    BufferAggregator agg = exampleSumAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[exampleSumAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Double result = (Double) agg.get(buffer, 0);

    Assert.assertEquals(253.6487, result, 0.0001);
    Assert.assertEquals( 253L, agg.getLong(buffer, 0));
    Assert.assertEquals(253.6487, agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Float f1 = 3.0f;
    Float f2 = 4.0f;
    Assert.assertEquals((double) (f1+f2), exampleSumAggFactory.combine(f1, f2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    Float f1 = 3.0f;
    Float f2 = null;
    Comparator comparator = exampleSumAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(f1, f2));
    Assert.assertEquals(0, comparator.compare(f1, f1));
    Assert.assertEquals(0, comparator.compare(f2, f2));
    Assert.assertEquals(-1, comparator.compare(f2, f1));
  }

  @Test
  public void testFloatAnyCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[exampleSumAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Double result = (Double) agg.get(buffer, 0);

    Assert.assertEquals(257.6487, result, 0.0001);
    Assert.assertEquals(257, agg.getLong(buffer, 0));
    Assert.assertEquals(257.6487, agg.getFloat(buffer, 0), 0.0001);
  }

  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      BufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    valueSelector.increment();
    objectSelector.increment();
  }
}
