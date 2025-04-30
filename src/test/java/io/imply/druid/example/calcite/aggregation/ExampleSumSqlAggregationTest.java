package io.imply.druid.example.calcite.aggregation;

import com.google.common.collect.ImmutableList;
import io.imply.druid.example.ExampleExtensionModule;
import io.imply.druid.example.aggregator.ExampleSumAggregatorFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SqlTestFrameworkConfig.ComponentSupplier(ExampleSumSqlAggregationTest.MyComponentSupplier.class)
public class ExampleSumSqlAggregationTest extends BaseCalciteQueryTest
{

  public static class MyComponentSupplier extends SqlTestFramework.StandardComponentSupplier
  {

    public MyComponentSupplier(TempDirProducer tempDirProducer)
    {
      super(tempDirProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(
          super.getCoreModule(),
          new ExampleExtensionModule()
      );
    }
  }

  @Test
  public void testExampleSumSql()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(m1) from foo")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "m1")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[] {21.0F}))
        .run();
  }

  @Test
  public void testExampleSumSqlLong()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(l1) from numfoo where l1<10")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE3)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .filters(range("l1", ColumnType.LONG, null, 10, false, true))
                    .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "l1")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[] {7L}))
        .run();
  }

  @Test
  public void testExampleSumSqlLiteral()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(1+1)")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(
                        InlineDataSource.fromIterable(
                            ImmutableList.of(new Object[] {2L}),
                            RowSignature.builder()
                                .add("$f0", ColumnType.LONG)
                                .build()
                        )
                    )
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .virtualColumns(expressionVirtualColumn("v0", "2", ColumnType.LONG))
                    .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "v0")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[] {2}))
        .run();
  }

  @Test
  public void testExampleSumSqlVirtualColumn()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(m1+1) from foo")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .virtualColumns(expressionVirtualColumn("v0", "(\"m1\" + 1)", ColumnType.FLOAT))
                    .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "v0")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[] {27.0F}))
        .run();
  }

  @Test
  public void testExampleSumSqlOnVarchar()
  {
    cannotVectorize();
    DruidException e = assertThrows(
        DruidException.class,
        () -> testBuilder()
            .sql("select EXAMPLE_SUM(dim1) from foo")
            .run()
    );
    assertTrue(e.getMessage().contains("Cannot apply 'EXAMPLE_SUM' to arguments of type 'EXAMPLE_SUM(<VARCHAR>)'"));
  }

  @Test
  public void testExampleSumSqlWithDistinct()
  {
    cannotVectorize();
    DruidException e = assertThrows(
        DruidException.class,
        () -> testBuilder()
            .sql("select EXAMPLE_SUM(distinct m1) from foo")
            .run()
    );
    assertTrue(e.getMessage().contains("not supported when useApproximateCountDistinct"));

  }

  @Test
  public void testExampleSumDouble()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(dbl1) from numfoo")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE3)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "dbl1")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[] {2.7D}))
        .run();
  }

  @Test
  public void testExampleSumMVD()
  {
    cannotVectorize();
    testBuilder()
        .sql("select example_sum(STRLEN(dim2)), example_sum(STRLEN(MV_TO_STRING(dim3, ''))) from numfoo")
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE3)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .virtualColumns(
                        expressionVirtualColumn("v0", "strlen(\"dim2\")", ColumnType.LONG),
                        expressionVirtualColumn("v1", "strlen(array_to_string(\"dim3\",''))", ColumnType.LONG)
                    )
                    .aggregators(
                        new ExampleSumAggregatorFactory("a0", "v0"),
                        new ExampleSumAggregatorFactory("a1", "v1")
                    )
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
            )
        )
        // dim2 - a, b, abc (non empty)
        // dim3 - [a, b], [b, c], d
        .expectedResults(ImmutableList.of(new Object[] {5, 5}))
        .run();
  }
}
