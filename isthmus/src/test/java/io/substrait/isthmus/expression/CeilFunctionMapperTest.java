package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;

class CeilFunctionMapperTest extends PlanTestBase {
  final RexBuilder rexBuilder = builder.getRexBuilder();

  final CeilFunctionMapper mapper =
      new CeilFunctionMapper(
          DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions(), rexBuilder);

  @Test
  void testCeilRealToSubstrait() {
    final RexNode real =
        rexBuilder.makeLiteral(
            123.456, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.REAL));
    final RexNode ceilRealCall = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, real);

    SubstraitFunctionMapping substraitFn = mapper.toSubstrait((RexCall) ceilRealCall).get();

    assertEquals("ceil", substraitFn.substraitName());
    assertEquals(List.of(real), substraitFn.operands());
  }

  @Test
  void testCeilDoubleToSubstrait() {
    final RexNode doubleValue =
        rexBuilder.makeLiteral(
            123.456, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
    final RexNode ceilRealCall = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, doubleValue);

    SubstraitFunctionMapping substraitFn = mapper.toSubstrait((RexCall) ceilRealCall).get();

    assertEquals("ceil", substraitFn.substraitName());
    assertEquals(List.of(doubleValue), substraitFn.operands());
  }

  @Test
  void testCeilTimestampToSubstrait() {
    final RexNode timestamp =
        rexBuilder.makeTimestampLiteral(
            new TimestampString(2026, 3, 2, 15, 30, 0).withNanos(123456789), 9);
    final RexNode unit = rexBuilder.makeFlag(TimeUnitRange.MONTH);
    final RexNode ceilTimestampCall =
        rexBuilder.makeCall(SqlStdOperatorTable.CEIL, timestamp, unit);

    SubstraitFunctionMapping substraitFn = mapper.toSubstrait((RexCall) ceilTimestampCall).get();

    assertEquals("round_calendar", substraitFn.substraitName());
    assertEquals(
        List.of(
            timestamp,
            rexBuilder.makeFlag(DatetimeRoundingMode.CEIL),
            unit,
            unit,
            rexBuilder.makeLiteral(
                1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))),
        substraitFn.operands());
  }

  @Test
  void testCeilDateToSubstrait() {
    final RexNode date = rexBuilder.makeDateLiteral(new DateString(2026, 3, 2));
    final RexNode unit = rexBuilder.makeFlag(TimeUnitRange.MONTH);
    final RexNode ceilDateCall = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, date, unit);

    SubstraitFunctionMapping substraitFn = mapper.toSubstrait((RexCall) ceilDateCall).get();

    assertEquals("round_calendar", substraitFn.substraitName());
    assertEquals(
        List.of(
            date,
            rexBuilder.makeFlag(SqlKind.CEIL),
            unit,
            unit,
            rexBuilder.makeLiteral(
                1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
            rexBuilder.makeNullLiteral(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE))),
        substraitFn.operands());
  }

  @Test
  void testCeilTimeToSubstrait() {
    final RexNode time =
        rexBuilder.makeTimeLiteral(new TimeString(15, 30, 0).withNanos(123456789), 9);
    final RexNode unit = rexBuilder.makeFlag(TimeUnitRange.MINUTE);
    final RexNode ceilTimeCall = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, time, unit);

    SubstraitFunctionMapping substraitFn = mapper.toSubstrait((RexCall) ceilTimeCall).get();

    assertEquals("round_calendar", substraitFn.substraitName());
    assertEquals(
        List.of(
            time,
            rexBuilder.makeFlag(SqlKind.CEIL),
            unit,
            unit,
            rexBuilder.makeLiteral(
                1L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
            rexBuilder.makeNullLiteral(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIME))),
        substraitFn.operands());
  }

  @Test
  void substraitCeilFp32GetExpressionArguments() {
    final Expression.FP32Literal fp32Value = ExpressionCreator.fp32(false, 123.456f);
    ScalarFunctionInvocation ceilFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ROUNDING,
            "ceil:fp32",
            TypeCreator.REQUIRED.FP32,
            fp32Value);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(ceilFn);

    assertEquals(List.of(fp32Value), arguments.get());
  }

  @Test
  void substraitCeilFp64GetExpressionArguments() {
    final Expression.FP64Literal fp64Value = ExpressionCreator.fp64(false, 123.456);
    ScalarFunctionInvocation ceilFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ROUNDING,
            "ceil:fp64",
            TypeCreator.REQUIRED.FP64,
            fp64Value);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(ceilFn);

    assertEquals(List.of(fp64Value), arguments.get());
  }

  @Test
  void substraitRoundCalendarCeilTimestampGetExpressionArguments() {
    final Expression.PrecisionTimestampLiteral timestamp =
        ExpressionCreator.precisionTimestamp(false, 1000L, 9);
    final EnumArg roundingArg = EnumArg.of("CEIL");
    final EnumArg unitArg = EnumArg.of("MONTH");
    final EnumArg originArg = EnumArg.of("MONTH");
    final Expression.I64Literal multipleArg = ExpressionCreator.i64(false, 1L);
    ScalarFunctionInvocation roundCalendarFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "round_calendar:pts_req_req_req_i64",
            TypeCreator.REQUIRED.precisionTimestamp(9),
            timestamp,
            roundingArg,
            unitArg,
            originArg,
            multipleArg);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(roundCalendarFn);

    assertEquals(List.of(timestamp, unitArg), arguments.get());
  }

  @Test
  void substraitRoundCalendarCeilDateGetExpressionArguments() {
    final Expression.DateLiteral date = ExpressionCreator.date(false, 20270);
    final EnumArg roundingArg = EnumArg.of("CEIL");
    final EnumArg unitArg = EnumArg.of("MONTH");
    final EnumArg originArg = EnumArg.of("MONTH");
    final Expression.I64Literal multipleArg = ExpressionCreator.i64(false, 1L);
    final Expression.DateLiteral originDate = ExpressionCreator.date(false, 0);
    ScalarFunctionInvocation roundCalendarFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "round_calendar:date_req_req_req_i64_date",
            TypeCreator.REQUIRED.DATE,
            date,
            roundingArg,
            unitArg,
            originArg,
            multipleArg,
            originDate);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(roundCalendarFn);

    assertEquals(List.of(date, unitArg), arguments.get());
  }

  @Test
  void substraitRoundCalendarCeilTimeGetExpressionArguments() {
    final Expression.TimeLiteral time = ExpressionCreator.time(false, 55800000000000L);
    final EnumArg roundingArg = EnumArg.of("CEIL");
    final EnumArg unitArg = EnumArg.of("MINUTE");
    final EnumArg originArg = EnumArg.of("MINUTE");
    final Expression.I64Literal multipleArg = ExpressionCreator.i64(false, 1L);
    final Expression.TimeLiteral originTime = ExpressionCreator.time(false, 0L);
    ScalarFunctionInvocation roundCalendarFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "round_calendar:time_req_req_req_i64_time",
            TypeCreator.REQUIRED.precisionTime(9),
            time,
            roundingArg,
            unitArg,
            originArg,
            multipleArg,
            originTime);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(roundCalendarFn);

    assertEquals(List.of(time, unitArg), arguments.get());
  }

  @Test
  void substraitRoundCalendarFloorDoesNotMatchCeil() {
    final Expression.PrecisionTimestampLiteral timestamp =
        ExpressionCreator.precisionTimestamp(false, 1000L, 9);
    final EnumArg roundingArg = EnumArg.of("FLOOR");
    final EnumArg unitArg = EnumArg.of("MONTH");
    final EnumArg originArg = EnumArg.of("MONTH");
    final Expression.I64Literal multipleArg = ExpressionCreator.i64(false, 1L);
    ScalarFunctionInvocation roundCalendarFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "round_calendar:pts_req_req_req_i64",
            TypeCreator.REQUIRED.precisionTimestamp(9),
            timestamp,
            roundingArg,
            unitArg,
            originArg,
            multipleArg);

    Optional<List<FunctionArg>> arguments = mapper.getExpressionArguments(roundCalendarFn);

    assertTrue(arguments.isEmpty());
  }

  @Test
  void ceilTimestampRoundTripTest() throws SqlParseException {
    DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions().stream()
        .filter(f -> "round_calendar".equals(f.name()))
        .forEach(f -> System.out.println(f.getAnchor().key()));

    assertFullRoundTrip("SELECT CEIL(timestamp'2022-01-01 12:34:56.789' TO MONTH) FROM orders");
  }
}
