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

package io.imply.druid.example.expression;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;

import java.util.List;

/**
 *
 */
public class ExampleSumExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "example_sum";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    final Expr arg = args.get(0);
    final Expr toAddArg = args.get(1);

    validationHelperCheckArgIsLiteral(toAddArg, "ARGUMENT_TO_ADD");
    if (!(toAddArg.getLiteralValue() instanceof Number)) {
      throw validationFailed("Argument to add must be a number");
    }

    return new ExampleSumExpr(FN_NAME, arg, toAddArg, shuttle -> shuttle.visit(apply(shuttle.visitAll(args))));
  }
}

