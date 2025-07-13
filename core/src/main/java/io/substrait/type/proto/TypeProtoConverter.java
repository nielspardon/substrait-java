package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Type;

/** Convert from {@link io.substrait.type.Type} to {@link io.substrait.proto.Type} */
public class TypeProtoConverter extends BaseProtoConverter<Type, Integer> {

  public TypeProtoConverter(final ExtensionCollector extensionCollector) {
    super(extensionCollector, "Type literals cannot contain parameters or expressions.");
  }

  public io.substrait.proto.Type toProto(final io.substrait.type.Type type) {
    return type.accept(this);
  }

  private static final BaseProtoTypes<Type, Integer> NULLABLE =
      new Types(Type.Nullability.NULLABILITY_NULLABLE);
  private static final BaseProtoTypes<Type, Integer> REQUIRED =
      new Types(Type.Nullability.NULLABILITY_REQUIRED);

  @Override
  public BaseProtoTypes<Type, Integer> typeContainer(final boolean nullable) {
    return nullable ? NULLABLE : REQUIRED;
  }

  private static class Types extends BaseProtoTypes<Type, Integer> {

    public Types(final Type.Nullability nullability) {
      super(nullability);
    }

    public Type fixedChar(final Integer len) {
      return wrap(Type.FixedChar.newBuilder().setLength(len).setNullability(nullability).build());
    }

    @Override
    public Type typeParam(final String name) {
      throw new UnsupportedOperationException(
          "It is not possible to use parameters in basic types.");
    }

    @Override
    public Integer integerParam(final String name) {
      throw new UnsupportedOperationException(
          "It is not possible to use parameters in basic types.");
    }

    public Type varChar(final Integer len) {
      return wrap(Type.VarChar.newBuilder().setLength(len).setNullability(nullability).build());
    }

    public Type fixedBinary(final Integer len) {
      return wrap(Type.FixedBinary.newBuilder().setLength(len).setNullability(nullability).build());
    }

    public Type decimal(final Integer scale, final Integer precision) {
      return wrap(
          Type.Decimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type intervalDay(final Integer precision) {
      return wrap(
          Type.IntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type intervalCompound(final Integer precision) {
      return wrap(
          Type.IntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type precisionTime(final Integer precision) {
      return wrap(
          Type.PrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type precisionTimestamp(final Integer precision) {
      return wrap(
          Type.PrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type precisionTimestampTZ(final Integer precision) {
      return wrap(
          Type.PrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type struct(final Iterable<Type> types) {
      return wrap(Type.Struct.newBuilder().addAllTypes(types).setNullability(nullability).build());
    }

    public Type list(final Type type) {
      return wrap(Type.List.newBuilder().setType(type).setNullability(nullability).build());
    }

    public Type map(final Type key, final Type value) {
      return wrap(
          Type.Map.newBuilder().setKey(key).setValue(value).setNullability(nullability).build());
    }

    @Override
    public Type userDefined(final int ref) {
      return wrap(
          Type.UserDefined.newBuilder().setTypeReference(ref).setNullability(nullability).build());
    }

    @Override
    protected Type wrap(final Object o) {
      final var bldr = Type.newBuilder();
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
      } else if (o instanceof final Type.IntervalDay t) {
        return bldr.setIntervalDay(t).build();
      } else if (o instanceof final Type.IntervalCompound t) {
        return bldr.setIntervalCompound(t).build();
      } else if (o instanceof final Type.FixedChar t) {
        return bldr.setFixedChar(t).build();
      } else if (o instanceof final Type.VarChar t) {
        return bldr.setVarchar(t).build();
      } else if (o instanceof final Type.FixedBinary t) {
        return bldr.setFixedBinary(t).build();
      } else if (o instanceof final Type.Decimal t) {
        return bldr.setDecimal(t).build();
      } else if (o instanceof final Type.PrecisionTimestamp t) {
        return bldr.setPrecisionTimestamp(t).build();
      } else if (o instanceof final Type.PrecisionTimestampTZ t) {
        return bldr.setPrecisionTimestampTz(t).build();
      } else if (o instanceof final Type.Struct t) {
        return bldr.setStruct(t).build();
      } else if (o instanceof final Type.List t) {
        return bldr.setList(t).build();
      } else if (o instanceof final Type.Map t) {
        return bldr.setMap(t).build();
      } else if (o instanceof final Type.UUID t) {
        return bldr.setUuid(t).build();
      } else if (o instanceof final Type.UserDefined t) {
        return bldr.setUserDefined(t).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected Integer i(final int integerValue) {
      return integerValue;
    }
  }
}
