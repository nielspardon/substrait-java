package io.substrait.isthmus.expression;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Maps Calcite CEIL function calls to Substrait ceil and round_calendar functions.
 *
 * <p>This mapper handles two types of CEIL operations:
 *
 * <ul>
 *   <li>Single-argument ceil: Maps directly to Substrait's ceil function for numeric types
 *   <li>Two-argument ceil: Maps to Substrait's round_calendar function for temporal types
 *       (TIMESTAMP, DATE, TIME) with CEIL rounding mode
 * </ul>
 *
 * <p>The mapper also provides reverse mapping from Substrait expressions back to Calcite function
 * arguments.
 */
public class CeilFunctionMapper implements ScalarFunctionMapper {
  private static final String CEIL_FUNCTION_NAME = "ceil";
  private static final String ROUND_CALENDAR_FUNCTION_NAME = "round_calendar";

  private final List<ScalarFunctionVariant> ceilFunctionVariants;
  private final List<ScalarFunctionVariant> roundCalendarFunctionVariants;
  private final RexBuilder rexBuilder;

  /**
   * Constructs a CeilFunctionMapper with the specified function variants and RexBuilder.
   *
   * @param functions the list of all available scalar function variants to filter from
   * @param rexBuilder the RexBuilder used to construct Calcite expressions
   */
  public CeilFunctionMapper(List<ScalarFunctionVariant> functions, RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    ceilFunctionVariants =
        functions.stream()
            .filter(f -> CEIL_FUNCTION_NAME.equals(f.name()))
            .collect(Collectors.toList());
    roundCalendarFunctionVariants =
        functions.stream()
            .filter(f -> ROUND_CALENDAR_FUNCTION_NAME.equals(f.name()))
            .collect(Collectors.toList());
  }

  /**
   * Converts a Calcite CEIL function call to a Substrait function mapping.
   *
   * <p>Handles the following cases:
   *
   * <ul>
   *   <li>Single operand: Maps to Substrait ceil function (e.g., CEIL(numeric_value))
   *   <li>Two operands with TIMESTAMP: Maps to round_calendar with CEIL mode
   *   <li>Two operands with DATE: Maps to round_calendar with CEIL mode and DATE type hint
   *   <li>Two operands with TIME: Maps to round_calendar with CEIL mode and TIME type hint
   * </ul>
   *
   * @param call the Calcite RexCall representing a CEIL function invocation
   * @return an Optional containing the SubstraitFunctionMapping if the call is a CEIL operation,
   *     otherwise empty
   * @throws UnsupportedOperationException if the CEIL call has an unsupported number of operands
   */
  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (!SqlStdOperatorTable.CEIL.equals(call.op)) {
      return Optional.empty();
    }

    if (call.operandCount() == 1) {
      return Optional.of(
          new SubstraitFunctionMapping(
              CEIL_FUNCTION_NAME, List.of(call.operands.get(0)), ceilFunctionVariants));
    } else if (call.operandCount() == 2) {
      switch (call.operands.get(0).getType().getSqlTypeName()) {
        case TIMESTAMP:
          // round_calendar:timestamp_req_req_req_i64
          return Optional.of(
              new SubstraitFunctionMapping(
                  ROUND_CALENDAR_FUNCTION_NAME,
                  List.of(
                      call.operands.get(0),
                      rexBuilder.makeFlag(DatetimeRoundingMode.CEIL),
                      call.operands.get(1),
                      call.operands.get(1),
                      rexBuilder.makeLiteral(
                          1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))),
                  roundCalendarFunctionVariants));
        case DATE:
          // round_calendar:date_req_req_req_i64_date
          return Optional.of(
              new SubstraitFunctionMapping(
                  ROUND_CALENDAR_FUNCTION_NAME,
                  List.of(
                      call.operands.get(0),
                      rexBuilder.makeFlag(DatetimeRoundingMode.CEIL),
                      call.operands.get(1),
                      call.operands.get(1),
                      rexBuilder.makeLiteral(
                          1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                      rexBuilder.makeNullLiteral(
                          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE))),
                  roundCalendarFunctionVariants));
        case TIME:
          // round_calendar:time_req_req_req_i64_time
          return Optional.of(
              new SubstraitFunctionMapping(
                  ROUND_CALENDAR_FUNCTION_NAME,
                  List.of(
                      call.operands.get(0),
                      rexBuilder.makeFlag(DatetimeRoundingMode.CEIL),
                      call.operands.get(1),
                      call.operands.get(1),
                      rexBuilder.makeLiteral(
                          1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                      rexBuilder.makeNullLiteral(
                          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIME))),
                  roundCalendarFunctionVariants));
        default:
          return Optional.empty();
      }
    }

    throw new UnsupportedOperationException("Unsupported Calcite 'ceil' function call.");
  }

  /**
   * Extracts the appropriate function arguments from a Substrait scalar function invocation for
   * conversion back to Calcite.
   *
   * <p>This method handles two scenarios:
   *
   * <ul>
   *   <li>Direct ceil function: Returns all arguments as-is
   *   <li>round_calendar function with CEIL mode: Extracts the value and time unit arguments
   *       (positions 0 and 2) for temporal types (Timestamp, PrecisionTimestamp, Time,
   *       PrecisionTime, Date)
   * </ul>
   *
   * @param expression the Substrait scalar function invocation to extract arguments from
   * @return an Optional containing the list of function arguments if this is a ceil-related
   *     expression, otherwise empty
   */
  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(ScalarFunctionInvocation expression) {
    if (CEIL_FUNCTION_NAME.equals(expression.declaration().name())) {
      return Optional.of(expression.arguments());
    } else if (ROUND_CALENDAR_FUNCTION_NAME.equals(expression.declaration().name())) {
      final boolean isCeilFunctionCall =
          expression.arguments().get(1) instanceof EnumArg
              && CEIL_FUNCTION_NAME.equalsIgnoreCase(
                  ((EnumArg) expression.arguments().get(1)).value().get());
      if (isCeilFunctionCall && expression.arguments().get(0) instanceof Expression) {
        Type returnType = expression.outputType();
        if (returnType instanceof Type.Timestamp) {
          return Optional.of(List.of(expression.arguments().get(0), expression.arguments().get(2)));
        } else if (returnType instanceof Type.PrecisionTimestamp) {
          return Optional.of(List.of(expression.arguments().get(0), expression.arguments().get(2)));
        } else if (returnType instanceof Type.Time) {
          return Optional.of(List.of(expression.arguments().get(0), expression.arguments().get(2)));
        } else if (returnType instanceof Type.PrecisionTime) {
          return Optional.of(List.of(expression.arguments().get(0), expression.arguments().get(2)));
        } else if (returnType instanceof Type.Date) {
          return Optional.of(List.of(expression.arguments().get(0), expression.arguments().get(2)));
        }
      }
    }

    return Optional.empty();
  }
}
