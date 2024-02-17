package io.imply.druid.example.calcite.aggregation;

import com.google.common.collect.ImmutableList;
import io.imply.druid.example.ExampleExtensionModule;
import io.imply.druid.example.aggregator.ExampleSumAggregatorFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class ExampleSumSqlAggregationTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new ExampleExtensionModule());
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
        .expectedResults(ImmutableList.of(new Object[]{21.0}))
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
                      .virtualColumns(expressionVirtualColumn("v0", "(\"m1\" + 1)", ColumnType.DOUBLE))
                      .aggregators(aggregators(new ExampleSumAggregatorFactory("a0", "v0")))
                      .context(QUERY_CONTEXT_DEFAULT)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{27.0}))
        .run();
  }

  @Test
  public void testExampleSumSqlOnVarchar()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(dim1) from foo")
        .expectedException(expected -> expected.expect(DruidException.class))
        .run();
  }

  @Test
  public void testExampleSumSqlWithDistinct()
  {
    cannotVectorize();
    testBuilder()
        .sql("select EXAMPLE_SUM(distinct m1) from foo")
        .expectedException(expected -> expected.expect(DruidException.class))
        .run();
  }
}
