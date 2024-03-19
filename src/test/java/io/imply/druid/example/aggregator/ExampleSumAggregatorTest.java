package io.imply.druid.example.aggregator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
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
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubles = {1.1897, 0.001, 86.23, 166.228};
  private Double[] objects = {2.1897, 1.001, 87.23, 167.228};

  @Before
  public void setup()
  {
    exampleSumAggFactory = new ExampleSumAggregatorFactory("billy", "nilly");
    combiningAggFactory = (ExampleSumAggregatorFactory) exampleSumAggFactory.getCombiningFactory();
    valueSelector = new TestDoubleColumnSelectorImpl(doubles);
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
    Assert.assertEquals(253.6487, agg.getDouble(), 0.0001);
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
    Assert.assertEquals(253.6487, agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Double d1 = 3.0;
    Double d2 = 4.0;
    Assert.assertEquals((double) (d1+d2), exampleSumAggFactory.combine(d1, d2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    Double d1 = 3.0;
    Double d2 = null;
    Comparator comparator = exampleSumAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(d1, d2));
    Assert.assertEquals(0, comparator.compare(d1, d1));
    Assert.assertEquals(0, comparator.compare(d2, d2));
    Assert.assertEquals(-1, comparator.compare(d2, d1));
  }

  @Test
  public void testDoubleAnyCombiningBufferAggregator()
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
    Assert.assertEquals(257.6487, agg.getDouble(buffer, 0), 0.0001);
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
