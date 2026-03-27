# Substrait Java SDK

## Types

Most types in the Substrait Java SDK can be created by using the [`TypeCreator`](src/main/java/io/substrait/type/TypeCreator.java) utility class. `TypeCreator` contains two static instances of itself for nullable `TypeCreator.NULLABLE` and non-nullable `TypeCreator.REQUIRED` types.

### Simple Types

| Type Name      | Nullable Type                        | Non-Nullable Type.                   |
| -------------- | ------------------------------------ | ------------------------------------ |
| boolean        | `TypeCreator.NULLABLE.BOOLEAN`       | `TypeCreator.REQUIRED.BOOLEAN`       |
| i8             | `TypeCreator.NULLABLE.I8`            | `TypeCreator.REQUIRED.I8`            |
| i16            | `TypeCreator.NULLABLE.I16`           | `TypeCreator.REQUIRED.I16`           |
| i32            | `TypeCreator.NULLABLE.I16`           | `TypeCreator.REQUIRED.I32`           |
| i64            | `TypeCreator.NULLABLE.I64`           | `TypeCreator.REQUIRED.I64`           |
| fp32           | `TypeCreator.NULLABLE.FP32`          | `TypeCreator.REQUIRED.FP32`          |
| fp64           | `TypeCreator.NULLABLE.FP64`          | `TypeCreator.REQUIRED.F64`           |
| string         | `TypeCreator.NULLABLE.STRING`        | `TypeCreator.REQUIRED.STRING`        |
| binary         | `TypeCreator.NULLABLE.BINARY`        | `TypeCreator.REQUIRED.BINARY`        |
| timestamp      | `TypeCreator.NULLABLE.TIMESTAMP`     | `TypeCreator.REQUIRED.TIMESTAMP`     |
| timestamp_tz   | `TypeCreator.NULLABLE.TIMESTAMP_TZ`  | `TypeCreator.REQUIRED.TIMESTAMP_TZ`  |
| date           | `TypeCreator.NULLABLE.DATE`          | `TypeCreator.REQUIRED.DATE`          |
| time           | `TypeCreator.NULLABLE.TIME`          | `TypeCreator.REQUIRED.TIME`          |
| interval_year  | `TypeCreator.NULLABLE.INTERVAL_YEAR` | `TypeCreator.REQUIRED.INTERVAL_YEAR` |
| uuid           | `TypeCreator.NULLABLE.UUID`          | `TypeCreator.REQUIRED.UUID`          |


### Compound Types

| Type Name                 | Nullable Type                                               | Non-Nullable Type                                           |
| ------------------------- | ----------------------------------------------------------- | ----------------------------------------------------------- |
| FIXEDCHAR<L>              | `TypeCreator.NULLABLE.fixedChar(int len)`                   | `TypeCreator.REQUIRED.fixedChar(int len)`                   |
| VARCHAR<L>                | `TypeCreator.NULLABLE.varChar(int len)`                     | `TypeCreator.REQUIRED.varChar(int len)`                     |
| FIXEDBINARY<L>            | `TypeCreator.NULLABLE.fixedBinary(int len)`                 | `TypeCreator.REQUIRED.fixedBinary(int len)`                 |
| DECIMAL<P, S>             | `TypeCreator.NULLABLE.decimal(int precision, int scale)`    | `TypeCreator.REQUIRED.decimal(int precision, int scale)`    |
| STRUCT<T1,…,Tn>           | `TypeCreator.NULLABLE.struct(Type... types)`                | `TypeCreator.REQUIRED.struct(Type... types)`                |
| NSTRUCT<N:T1,…,N:Tn>      | always non-nullable                                         | `NamedStruct.of(Iterable<String> names, Type.Struct type)`  |
| LIST<T>                   | `TypeCreator.NULLABLE.list(Type type)`                      | `TypeCreator.REQUIRED.list(Type type)`                      |
| MAP<K, V>                 | `TypeCreator.NULLABLE.map(Type key, Type value)`            | `TypeCreator.REQUIRED.map(Type key, Type value)`            |
| FUNC<T->R>                | not yet implemented                                         | not yet implemented                                         |
| FUNC<(T1,…,Tn)->R>        | not yet implemented                                         | not yet implemented                                         |
| PRECISION_TIME<P>         | `TypeCreator.NULLABLE.precisionTime(int precision)`         | `TypeCreator.REQUIRED.precisionTime(int precision)`         |
| PRECISION_TIMESTAMP<P>    | `TypeCreator.NULLABLE.precisionTimestamp(int precision)`    | `TypeCreator.REQUIRED.precisionTimestamp(int precision)`    |
| PRECISION_TIMESTAMP_TZ<P> | `TypeCreator.NULLABLE.precisionTimestampTZ(int precision)`  | `TypeCreator.REQUIRED.precisionTimestampTZ(int precision)`  |
| INTERVAL_DAY<P>           | `TypeCreator.NULLABLE.intervalDay(int precision)`           | `TypeCreator.REQUIRED.intervalDay(int precision)`           |
| INTERVAL_COMPOUND<P>      | `TypeCreator.NULLABLE.intervalCompound(int precision)`      | `TypeCreator.REQUIRED.intervalCompound(int precision)`      |

### User Defined Types

Simple, unparameterized user defined types can be instantiated using the following utility method in [`TypeCreator`](src/main/java/io/substrait/type/TypeCreator.java).

| Type Name                 | Nullable Type                                               | Non-Nullable Type                                           |
| ------------------------- | ----------------------------------------------------------- | ----------------------------------------------------------- |
| USER_DEFINED<URN, NAME>   | `TypeCreator.NULLABLE.userDefined(String urn, String name)` | `TypeCreator.REQUIRED.userDefined(String urn, String name)` |

You can construct user defined parameterized types using the builder in `io.substrait.type.Type.UserDefined.builder()`, e.g. given the following user defined type definition in YAML:

```yaml
---
urn: extension:org.example:vector
types:
  - name: vector
    parameters:
      - name: T
        type: dataType
    structure:
      x: T
      y: T
      z: T
```

You can create a type using `Type.UserDefined.builder()` like this:

```java
Type.Parameter typeParam =
    io.substrait.type.ImmutableType.ParameterDataType.builder().type(TypeCreator.REQUIRED.I8).build();

Type vectorOfInt8Type =
    Type.UserDefined.builder()
        .nullable(false)
        .urn("extension:org.example:vector")
        .name("vector")
        .addTypeParameters(typeParam)
        .build();
```

## Relations

tbd.

## Expressions

tbd.
