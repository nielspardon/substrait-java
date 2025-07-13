package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionVisitor;
import io.substrait.proto.ParameterizedType;
import io.substrait.proto.Type;

public class ParameterizedProtoConverter
    extends BaseProtoConverter<ParameterizedType, ParameterizedType.IntegerOption> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ParameterizedProtoConverter.class);

  public ParameterizedProtoConverter(final ExtensionCollector extensionCollector) {
    super(extensionCollector, "Parameterized types cannot include return type expressions.");
  }

  @Override
  public BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption> typeContainer(
      final boolean nullable) {
    return nullable ? PARAMETERIZED_NULLABLE : PARAMETERIZED_REQUIRED;
  }

  private static final BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption>
      PARAMETERIZED_NULLABLE = new ParameterizedTypes(Type.Nullability.NULLABILITY_NULLABLE);
  private static final BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption>
      PARAMETERIZED_REQUIRED = new ParameterizedTypes(Type.Nullability.NULLABILITY_REQUIRED);

  public ParameterizedType.IntegerOption i(final TypeExpression num) {
    return num.accept(new IntegerVisitor());
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.FixedChar expr)
      throws RuntimeException {
    return typeContainer(expr).fixedChar(expr.length().value());
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.VarChar expr)
      throws RuntimeException {
    return typeContainer(expr).varChar(expr.length().value());
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.FixedBinary expr)
      throws RuntimeException {
    return typeContainer(expr).fixedBinary(expr.length().value());
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.Decimal expr)
      throws RuntimeException {
    return typeContainer(expr).decimal(i(expr.precision()), i(expr.scale()));
  }

  @Override
  public ParameterizedType visit(
      final io.substrait.function.ParameterizedType.PrecisionTimestamp expr)
      throws RuntimeException {
    return typeContainer(expr).precisionTimestamp(i(expr.precision()));
  }

  @Override
  public ParameterizedType visit(
      final io.substrait.function.ParameterizedType.PrecisionTimestampTZ expr)
      throws RuntimeException {
    return typeContainer(expr).precisionTimestampTZ(i(expr.precision()));
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.Struct expr)
      throws RuntimeException {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.ListType expr)
      throws RuntimeException {
    return typeContainer(expr).list(expr.name().accept(this));
  }

  @Override
  public ParameterizedType visit(final io.substrait.function.ParameterizedType.Map expr)
      throws RuntimeException {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  @Override
  public ParameterizedType visit(
      final io.substrait.function.ParameterizedType.StringLiteral stringLiteral)
      throws RuntimeException {
    return ParameterizedType.newBuilder()
        .setTypeParameter(
            ParameterizedType.TypeParameter.newBuilder().setName(stringLiteral.value()))
        .build();
  }

  private static class IntegerVisitor
      extends TypeExpressionVisitor.TypeExpressionThrowsVisitor<
          ParameterizedType.IntegerOption, RuntimeException> {

    public IntegerVisitor() {
      super("Parameterized integer locations should only include integer names or literals.");
    }

    @Override
    public ParameterizedType.IntegerOption visit(final TypeExpression.IntegerLiteral literal) {
      return ParameterizedType.IntegerOption.newBuilder().setLiteral(literal.value()).build();
    }

    @Override
    public ParameterizedType.IntegerOption visit(
        final io.substrait.function.ParameterizedType.StringLiteral stringLiteral)
        throws RuntimeException {
      return ParameterizedType.IntegerOption.newBuilder()
          .setParameter(
              ParameterizedType.IntegerParameter.newBuilder().setName(stringLiteral.value()))
          .build();
    }
  }

  public static class ParameterizedTypes
      extends BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption> {

    public ParameterizedTypes(final Type.Nullability nullability) {
      super(nullability);
    }

    public ParameterizedType fixedChar(final ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedFixedChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType typeParam(final String name) {
      return ParameterizedType.newBuilder()
          .setTypeParameter(ParameterizedType.TypeParameter.newBuilder().setName(name))
          .build();
    }

    @Override
    public ParameterizedType.IntegerOption integerParam(final String name) {
      return ParameterizedType.IntegerOption.newBuilder()
          .setParameter(ParameterizedType.IntegerParameter.newBuilder().setName(name))
          .build();
    }

    protected ParameterizedType.IntegerOption i(final int len) {
      return ParameterizedType.IntegerOption.newBuilder().setLiteral(len).build();
    }

    private static ParameterizedType.IntegerOption i(final String param) {
      return ParameterizedType.IntegerOption.newBuilder()
          .setParameter(ParameterizedType.IntegerParameter.newBuilder().setName(param))
          .build();
    }

    public ParameterizedType varChar(final ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedVarChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public ParameterizedType fixedBinary(final ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedFixedBinary.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public ParameterizedType decimal(
        final ParameterizedType.IntegerOption scale,
        final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedDecimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType intervalDay(final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedIntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType intervalCompound(final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedIntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTime(final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTimestamp(final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTimestampTZ(final ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public ParameterizedType struct(final Iterable<ParameterizedType> types) {
      return wrap(
          ParameterizedType.ParameterizedStruct.newBuilder()
              .addAllTypes(types)
              .setNullability(nullability)
              .build());
    }

    public ParameterizedType list(final ParameterizedType type) {
      return wrap(
          ParameterizedType.ParameterizedList.newBuilder()
              .setType(type)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    public ParameterizedType map(final ParameterizedType key, final ParameterizedType value) {
      return wrap(
          ParameterizedType.ParameterizedMap.newBuilder()
              .setKey(key)
              .setValue(value)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    @Override
    public ParameterizedType userDefined(final int ref) {
      throw new UnsupportedOperationException(
          "User defined types are not supported in Parameterized Types for now");
    }

    @Override
    protected ParameterizedType wrap(final Object o) {
      final var bldr = ParameterizedType.newBuilder();
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
      } else if (o instanceof final ParameterizedType.ParameterizedIntervalDay t) {
        return bldr.setIntervalDay(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedIntervalCompound t) {
        return bldr.setIntervalCompound(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedFixedChar t) {
        return bldr.setFixedChar(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedVarChar t) {
        return bldr.setVarchar(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedFixedBinary t) {
        return bldr.setFixedBinary(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedDecimal t) {
        return bldr.setDecimal(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedPrecisionTimestamp t) {
        return bldr.setPrecisionTimestamp(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedPrecisionTimestampTZ t) {
        return bldr.setPrecisionTimestampTz(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedStruct t) {
        return bldr.setStruct(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedList t) {
        return bldr.setList(t).build();
      } else if (o instanceof final ParameterizedType.ParameterizedMap t) {
        return bldr.setMap(t).build();
      } else if (o instanceof final Type.UUID t) {
        return bldr.setUuid(t).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }
  }
}
