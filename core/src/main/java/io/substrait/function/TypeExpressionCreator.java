package io.substrait.function;

import io.substrait.type.TypeCreator;
import java.util.Arrays;

public class TypeExpressionCreator extends TypeCreator
    implements ExtendedTypeCreator<TypeExpression, TypeExpression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TypeExpressionCreator.class);

  public static final TypeExpressionCreator REQUIRED = new TypeExpressionCreator(false);
  public static final TypeExpressionCreator NULLABLE = new TypeExpressionCreator(true);

  protected TypeExpressionCreator(final boolean nullable) {
    super(nullable);
  }

  public TypeExpression fixedCharE(final TypeExpression len) {
    return TypeExpression.FixedChar.builder().nullable(nullable).length(len).build();
  }

  public TypeExpression varCharE(final TypeExpression len) {
    return TypeExpression.VarChar.builder().nullable(nullable).length(len).build();
  }

  public TypeExpression fixedBinaryE(final TypeExpression len) {
    return TypeExpression.FixedBinary.builder().nullable(nullable).length(len).build();
  }

  public TypeExpression decimalE(final TypeExpression precision, final TypeExpression scale) {
    return TypeExpression.Decimal.builder()
        .nullable(nullable)
        .scale(scale)
        .precision(precision)
        .build();
  }

  public TypeExpression intervalDayE(final TypeExpression precision) {
    return TypeExpression.IntervalDay.builder().nullable(nullable).precision(precision).build();
  }

  public TypeExpression intervalCompoundE(final TypeExpression precision) {
    return TypeExpression.IntervalCompound.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  public TypeExpression precisionTimestampE(final TypeExpression precision) {
    return TypeExpression.PrecisionTimestamp.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  public TypeExpression precisionTimestampTZE(final TypeExpression precision) {
    return TypeExpression.PrecisionTimestampTZ.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  public TypeExpression structE(final TypeExpression... types) {
    return TypeExpression.Struct.builder().nullable(nullable).addFields(types).build();
  }

  public TypeExpression structE(final Iterable<? extends TypeExpression> types) {
    return TypeExpression.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  public TypeExpression listE(final TypeExpression type) {
    return TypeExpression.ListType.builder().nullable(nullable).elementType(type).build();
  }

  public TypeExpression mapE(final TypeExpression key, final TypeExpression value) {
    return TypeExpression.Map.builder().nullable(nullable).key(key).value(value).build();
  }

  public static class Assign {

    public Assign() {}

    public Assign(final String name, final TypeExpression expr) {
      this.name = name;
      this.expr = expr;
    }

    String name;
    TypeExpression expr;

    public String name() {
      return name;
    }

    public TypeExpression expr() {
      return expr;
    }
  }
  ;

  public static TypeExpression program(
      final TypeExpression finalExpr, final Assign... assignments) {
    return TypeExpression.ReturnProgram.builder()
        .finalExpression(finalExpr)
        .addAllAssignments(
            Arrays.stream(assignments)
                .map(
                    a ->
                        TypeExpression.ReturnProgram.Assignment.builder()
                            .name(a.name())
                            .expr(a.expr())
                            .build())
                .collect(java.util.stream.Collectors.toList()))
        .build();
  }

  public static TypeExpression plus(final TypeExpression left, final TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.ADD, left, right);
  }

  public static TypeExpression minus(final TypeExpression left, final TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.SUBTRACT, left, right);
  }

  public static TypeExpression binary(
      final TypeExpression.BinaryOperation.OpType op,
      final TypeExpression left,
      final TypeExpression right) {
    return TypeExpression.BinaryOperation.builder().opType(op).left(left).right(right).build();
  }

  public static TypeExpression.IntegerLiteral i(final int i) {
    return TypeExpression.IntegerLiteral.builder().value(i).build();
  }
}
