/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.example.calcite.aggregation;


import com.google.common.collect.Iterables;
import io.imply.druid.example.aggregator.ExampleSumAggregatorFactory;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This class serves as sql binding for EXAMPLE_SUM.
 *
 * It defines a {@link SqlAggFunction} for EXAMPLE_SUM which takes a Numeric operand as input
 * and converts the call to druid aggregation using {@link ExampleSumAggregatorFactory}
 */
public class ExampleSumSqlAggregator implements SqlAggregator
{
  public static final String NAME = "EXAMPLE_SUM";
  public static final SqlAggFunction FUNCTION_INSTANCE =
      OperatorConversions.aggregatorBuilder(NAME)
                         .operandTypes(SqlTypeFamily.NUMERIC)
                         .requiredOperandCount(1)
                         .literalOperands(1)
                         .returnTypeInference(ReturnTypes.ARG0)
                         .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
                         .build();

  /**
   * @return the user defined {@link SqlAggFunction}
   */
  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  /**
   * converts the call to an Aggregation
   */
  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    if (aggregateCall.isDistinct()) {
      return null;
    }

    final List<DruidExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        aggregateCall,
        inputAccessor
    );

    if (arguments == null) {
      return null;
    }

    // we expect only one argument to the example_sum function
    final DruidExpression arg = Iterables.getOnlyElement(arguments);
    final String fieldName;

    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, aggregateCall.getType());
    }

    return getAggregation(name, aggregateCall, fieldName);
  }

  @Nullable
  Aggregation getAggregation(
      String name,
      AggregateCall aggregateCall,
      String fieldName
  )
  {
    final ColumnType valueType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (valueType == null) {
      return null;
    }

    return Aggregation.create(new ExampleSumAggregatorFactory(name, fieldName));
  }
}
