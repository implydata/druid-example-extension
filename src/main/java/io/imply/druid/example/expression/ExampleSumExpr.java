package io.imply.druid.example.expression;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

class ExampleSumExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
{

  private final Expr toAddArg;
  private final Function<Shuttle, Expr> visitFunction;
  private final Function<Double, Double> addFunction;

  protected ExampleSumExpr(
      final String functionName,
      final Expr arg,
      final Expr toAddArg,
      final Function<Shuttle, Expr> visitFunction
  )
  {
    super(functionName, arg);
    this.toAddArg = toAddArg;
    this.visitFunction = visitFunction;
    this.addFunction = createFunction(toAddArg);
  }

  @Nonnull
  @Override
  public ExprEval<?> eval(final ObjectBinding bindings)
  {
    final DoubleList doubles = toDoubleArray(arg.eval(bindings));

    if (doubles == null) {
      return ExprEval.ofDouble(null);
    }

    final DoubleList addedDoubles = new DoubleArrayList(doubles.size());
    for (double num : doubles) {
      addedDoubles.add(addFunction.apply(num));
    }
    return ExprEval.ofDoubleArray(addedDoubles.toArray());
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return visitFunction.apply(shuttle);
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return ExpressionType.DOUBLE_ARRAY;
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("%s(%s, %s)", name, arg.stringify(), toAddArg.stringify());
  }

  private static Function<Double, Double> createFunction(Expr toAddExpr)
  {
    final double numberToAdd = ((Number) toAddExpr.getLiteralValue()).doubleValue();
    return num -> num + numberToAdd;
  }

  @Nullable
  static DoubleList toDoubleArray(final ExprEval<?> eval)
  {
    if (!eval.type().isArray() || !eval.type().getElementType().isNumeric()) {
      return null;
    }

    final Object[] arr = eval.asArray();

    if (arr == null) {
      return null;
    }

    // Copy array to double[], while verifying all elements are numbers and skipping nulls.
    final DoubleArrayList doubles = new DoubleArrayList(arr.length);

    for (final Object o : arr) {
      if (o != null) {
        doubles.add(((Number) o).doubleValue());
      }
    }

    return doubles;
  }
}
