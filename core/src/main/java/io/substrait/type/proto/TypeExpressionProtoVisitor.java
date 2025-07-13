package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.proto.DerivationExpression;
import io.substrait.proto.Type;

public class TypeExpressionProtoVisitor
    extends BaseProtoConverter<DerivationExpression, DerivationExpression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TypeExpressionProtoVisitor.class);

  public TypeExpressionProtoVisitor(final ExtensionCollector extensionCollector) {
    super(extensionCollector, "Unexpected expression type. This shouldn't happen.");
  }

  @Override
  public BaseProtoTypes<DerivationExpression, DerivationExpression> typeContainer(
      final boolean nullable) {
    return nullable ? DERIVATION_NULLABLE : DERIVATION_REQUIRED;
  }

  private static final DerivationTypes DERIVATION_NULLABLE =
      new DerivationTypes(Type.Nullability.NULLABILITY_NULLABLE);
  private static final DerivationTypes DERIVATION_REQUIRED =
      new DerivationTypes(Type.Nullability.NULLABILITY_REQUIRED);

  @Override
  public DerivationExpression visit(final TypeExpression.BinaryOperation expr) {
    final var opType =
        switch (expr.opType()) {
          case ADD -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_PLUS;
          case SUBTRACT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
          case MIN -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MIN;
          case MAX -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MAX;
          case LT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
            // case LTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
          case GT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_GREATER_THAN;
            // case GTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
            // case NOT_EQ -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQ;
          case EQ -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQUALS;
          case COVERS -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_COVERS;
          default -> throw new IllegalStateException("Unexpected value: " + expr.opType());
        };
    return DerivationExpression.newBuilder()
        .setBinaryOp(
            DerivationExpression.BinaryOp.newBuilder()
                .setArg1(expr.left().accept(this))
                .setArg2(expr.right().accept(this))
                .setOpType(opType)
                .build())
        .build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.NotOperation expr) {
    return DerivationExpression.newBuilder()
        .setUnaryOp(
            DerivationExpression.UnaryOp.newBuilder()
                .setOpType(DerivationExpression.UnaryOp.UnaryOpType.UNARY_OP_TYPE_BOOLEAN_NOT)
                .setArg(expr.inner().accept(this)))
        .build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.IfOperation expr) {
    return DerivationExpression.newBuilder()
        .setIfElse(
            DerivationExpression.IfElse.newBuilder()
                .setIfCondition(expr.ifCondition().accept(this))
                .setIfReturn(expr.thenExpr().accept(this))
                .setElseReturn(expr.elseExpr().accept(this))
                .build())
        .build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.IntegerLiteral expr) {
    return DerivationExpression.newBuilder().setIntegerLiteral(expr.value()).build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.ReturnProgram expr) {
    final var assignments =
        expr.assignments().stream()
            .map(
                a ->
                    DerivationExpression.ReturnProgram.Assignment.newBuilder()
                        .setName(a.name())
                        .setExpression(a.expr().accept(this))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    final var finalExpr = expr.finalExpression().accept(this);
    return DerivationExpression.newBuilder()
        .setReturnProgram(
            DerivationExpression.ReturnProgram.newBuilder()
                .setFinalExpression(finalExpr)
                .addAllAssignments(assignments)
                .build())
        .build();
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.FixedChar expr) {
    return typeContainer(expr).fixedChar(expr.length().value());
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.VarChar expr) {
    return typeContainer(expr).varChar(expr.length().value());
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.FixedBinary expr) {
    return typeContainer(expr).fixedBinary(expr.length().value());
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.Decimal expr) {
    return typeContainer(expr).decimal(expr.precision().accept(this), expr.scale().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.IntervalDay expr) {
    return typeContainer(expr).intervalDay(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.IntervalCompound expr) {
    return typeContainer(expr).intervalCompound(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.PrecisionTimestamp expr) {
    return typeContainer(expr).precisionTimestamp(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.PrecisionTimestampTZ expr) {
    return typeContainer(expr).precisionTimestampTZ(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.Struct expr) {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.ListType expr) {
    return typeContainer(expr).list(expr.name().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.Map expr) {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  @Override
  public DerivationExpression visit(final ParameterizedType.StringLiteral stringLiteral) {
    return DerivationExpression.newBuilder().setTypeParameterName(stringLiteral.value()).build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.FixedChar expr) {
    return typeContainer(expr).fixedChar(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.VarChar expr) {
    return typeContainer(expr).varChar(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.FixedBinary expr) {
    return typeContainer(expr).fixedBinary(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.Decimal expr) {
    return typeContainer(expr).decimal(expr.precision().accept(this), expr.scale().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.Struct expr) {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.ListType expr) {
    return typeContainer(expr).list(expr.elementType().accept(this));
  }

  @Override
  public DerivationExpression visit(final TypeExpression.Map expr) {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  private static class DerivationTypes
      extends BaseProtoTypes<DerivationExpression, DerivationExpression> {

    public DerivationTypes(final Type.Nullability nullability) {
      super(nullability);
    }

    public DerivationExpression fixedChar(final DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionFixedChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression typeParam(final String name) {
      return DerivationExpression.newBuilder().setTypeParameterName(name).build();
    }

    @Override
    public DerivationExpression integerParam(final String name) {
      return DerivationExpression.newBuilder().setIntegerParameterName(name).build();
    }

    public DerivationExpression varChar(final DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionVarChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression fixedBinary(final DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionFixedBinary.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression decimal(
        final DerivationExpression scale, final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionDecimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTime(final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTimestamp(final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTimestampTZ(final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression intervalDay(final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionIntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression intervalCompound(final DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionIntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression struct(final Iterable<DerivationExpression> types) {
      return wrap(
          DerivationExpression.ExpressionStruct.newBuilder()
              .addAllTypes(types)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression param(final String name) {
      return DerivationExpression.newBuilder().setTypeParameterName(name).build();
    }

    public DerivationExpression list(final DerivationExpression type) {
      return wrap(
          DerivationExpression.ExpressionList.newBuilder()
              .setType(type)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    public DerivationExpression map(
        final DerivationExpression key, final DerivationExpression value) {
      return wrap(
          DerivationExpression.ExpressionMap.newBuilder()
              .setKey(key)
              .setValue(value)
              .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
              .build());
    }

    @Override
    public DerivationExpression userDefined(final int ref) {
      throw new UnsupportedOperationException(
          "User defined types are not supported in Derivation Expressions for now");
    }

    @Override
    protected DerivationExpression wrap(final Object o) {
      final var bldr = DerivationExpression.newBuilder();
      if (o instanceof final Type.Boolean t) {
        return bldr.setBool(t).build();
      } else if (o instanceof final Type.I8 t) {
        return bldr.setI8(t).build();
      } else if (o instanceof final Type.I16 t) {
        return bldr.setI16(t).build();
      } else if (o instanceof final Type.I32 t) {
        return bldr.setI32(t).build();
      } else if (o instanceof final Type.I64 t) {
        return bldr.setI64(t).build();
      } else if (o instanceof final Type.FP32 t) {
        return bldr.setFp32(t).build();
      } else if (o instanceof final Type.FP64 t) {
        return bldr.setFp64(t).build();
      } else if (o instanceof final Type.String t) {
        return bldr.setString(t).build();
      } else if (o instanceof final Type.Binary t) {
        return bldr.setBinary(t).build();
      } else if (o instanceof final Type.Timestamp t) {
        return bldr.setTimestamp(t).build();
      } else if (o instanceof final Type.Date t) {
        return bldr.setDate(t).build();
      } else if (o instanceof final Type.Time t) {
        return bldr.setTime(t).build();
      } else if (o instanceof final Type.TimestampTZ t) {
        return bldr.setTimestampTz(t).build();
      } else if (o instanceof final Type.IntervalYear t) {
        return bldr.setIntervalYear(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionIntervalDay t) {
        return bldr.setIntervalDay(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionIntervalCompound t) {
        return bldr.setIntervalCompound(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionFixedChar t) {
        return bldr.setFixedChar(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionVarChar t) {
        return bldr.setVarchar(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionFixedBinary t) {
        return bldr.setFixedBinary(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionDecimal t) {
        return bldr.setDecimal(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionPrecisionTimestamp t) {
        return bldr.setPrecisionTimestamp(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionPrecisionTimestampTZ t) {
        return bldr.setPrecisionTimestampTz(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionStruct t) {
        return bldr.setStruct(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionList t) {
        return bldr.setList(t).build();
      } else if (o instanceof final DerivationExpression.ExpressionMap t) {
        return bldr.setMap(t).build();
      } else if (o instanceof final Type.UUID t) {
        return bldr.setUuid(t).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected DerivationExpression i(final int integerValue) {
      return DerivationExpression.newBuilder().setIntegerLiteral(integerValue).build();
    }
  }
}
