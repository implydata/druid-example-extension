package io.imply.druid.example.calcite.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Key;
import io.imply.druid.example.ExampleExtensionModule;
import io.imply.druid.example.aggregator.ExampleSumAggregatorFactory;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class ExampleSumSqlAggregationTest extends BaseCalciteQueryTest
{
  private static final ExampleExtensionModule MODULE = new ExampleExtensionModule();

  static {
    // throwaway, just using to properly initialize jackson modules
    Guice.createInjector(
        binder -> binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper()),
        MODULE
    );
  }

  @Test
  public void testExampleSumSql(){
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
        .run();
  }
}
