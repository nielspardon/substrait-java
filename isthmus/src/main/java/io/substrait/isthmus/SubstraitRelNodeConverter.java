package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.util.VisitationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter
    extends AbstractRelVisitor<RelNode, SubstraitRelNodeConverter.Context, RuntimeException> {

  protected final RelDataTypeFactory typeFactory;

  protected final ScalarFunctionConverter scalarFunctionConverter;
  protected final AggregateFunctionConverter aggregateFunctionConverter;
  protected final ExpressionRexConverter expressionRexConverter;

  protected final RelBuilder relBuilder;
  protected final RexBuilder rexBuilder;
  private final TypeConverter typeConverter;

  public SubstraitRelNodeConverter(
      final SimpleExtension.ExtensionCollection extensions,
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder) {
    this(
        typeFactory,
        relBuilder,
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory),
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
  }

  public SubstraitRelNodeConverter(
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder,
      final ScalarFunctionConverter scalarFunctionConverter,
      final AggregateFunctionConverter aggregateFunctionConverter,
      final WindowFunctionConverter windowFunctionConverter,
      final TypeConverter typeConverter) {
    this(
        typeFactory,
        relBuilder,
        scalarFunctionConverter,
        aggregateFunctionConverter,
        windowFunctionConverter,
        typeConverter,
        new ExpressionRexConverter(
            typeFactory, scalarFunctionConverter, windowFunctionConverter, typeConverter));
  }

  public SubstraitRelNodeConverter(
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder,
      final ScalarFunctionConverter scalarFunctionConverter,
      final AggregateFunctionConverter aggregateFunctionConverter,
      final WindowFunctionConverter windowFunctionConverter,
      final TypeConverter typeConverter,
      final ExpressionRexConverter expressionRexConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.relBuilder = relBuilder;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
    this.aggregateFunctionConverter = aggregateFunctionConverter;
    this.expressionRexConverter = expressionRexConverter;
    this.expressionRexConverter.setRelNodeConverter(this);
  }

  public static RelNode convert(
      final Rel relRoot,
      final RelOptCluster relOptCluster,
      final Prepare.CatalogReader catalogReader,
      final SqlParser.Config parserConfig) {
    final var relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    return relRoot.accept(
        new SubstraitRelNodeConverter(
            EXTENSION_COLLECTION, relOptCluster.getTypeFactory(), relBuilder),
        Context.newContext());
  }

  @Override
  public RelNode visit(final Filter filter, final Context context) throws RuntimeException {
    final RelNode input = filter.getInput().accept(this, context);
    final RexNode filterCondition = filter.getCondition().accept(expressionRexConverter, context);
    final RelNode node = relBuilder.push(input).filter(filterCondition).build();
    return applyRemap(node, filter.getRemap());
  }

  @Override
  public RelNode visit(final NamedScan namedScan, final Context context) throws RuntimeException {
    final RelNode node = relBuilder.scan(namedScan.getNames()).build();
    return applyRemap(node, namedScan.getRemap());
  }

  @Override
  public RelNode visit(final LocalFiles localFiles, final Context context) throws RuntimeException {
    return visitFallback(localFiles, context);
  }

  @Override
  public RelNode visit(final EmptyScan emptyScan, final Context context) throws RuntimeException {
    final RelDataType rowType =
        typeConverter.toCalcite(relBuilder.getTypeFactory(), emptyScan.getInitialSchema().struct());
    final RelNode node = LogicalValues.create(relBuilder.getCluster(), rowType, ImmutableList.of());
    return applyRemap(node, emptyScan.getRemap());
  }

  @Override
  public RelNode visit(final Project project, final Context context) throws RuntimeException {
    final RelNode child = project.getInput().accept(this, context);
    final Stream<RexNode> directOutputs =
        IntStream.range(0, child.getRowType().getFieldCount())
            .mapToObj(fieldIndex -> rexBuilder.makeInputRef(child, fieldIndex));

    final Stream<RexNode> exprs =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter, context));

    final List<RexNode> rexExprs =
        Stream.concat(directOutputs, exprs).collect(java.util.stream.Collectors.toList());

    final RelNode node = relBuilder.push(child).project(rexExprs).build();
    return applyRemap(node, project.getRemap());
  }

  @Override
  public RelNode visit(final Cross cross, final Context context) throws RuntimeException {
    final RelNode left = cross.getLeft().accept(this, context);
    final RelNode right = cross.getRight().accept(this, context);
    // Calcite represents CROSS JOIN as the equivalent INNER JOIN with true condition
    final RelNode node =
        relBuilder.push(left).push(right).join(JoinRelType.INNER, relBuilder.literal(true)).build();
    return applyRemap(node, cross.getRemap());
  }

  @Override
  public RelNode visit(final Join join, final Context context) throws RuntimeException {
    final RelNode left = join.getLeft().accept(this, context);
    final RelNode right = join.getRight().accept(this, context);
    final RexNode condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter, context))
            .orElse(relBuilder.literal(true));
    final var joinType =
        switch (join.getJoinType()) {
          case INNER -> JoinRelType.INNER;
          case LEFT -> JoinRelType.LEFT;
          case RIGHT -> JoinRelType.RIGHT;
          case OUTER -> JoinRelType.FULL;
          case SEMI -> JoinRelType.SEMI;
          case ANTI -> JoinRelType.ANTI;
          case LEFT_SEMI -> JoinRelType.SEMI;
          case LEFT_ANTI -> JoinRelType.ANTI;
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown join type is not supported");
          default -> throw new UnsupportedOperationException(
              "Unsupported join type: " + join.getJoinType().name());
        };
    final RelNode node = relBuilder.push(left).push(right).join(joinType, condition).build();
    return applyRemap(node, join.getRemap());
  }

  @Override
  public RelNode visit(final Set set, final Context context) throws RuntimeException {
    final int numInputs = set.getInputs().size();
    set.getInputs()
        .forEach(
            input -> {
              relBuilder.push(input.accept(this, context));
            });
    // TODO: MINUS_MULTISET and INTERSECTION_PRIMARY mappings are set to be removed as they do not
    //   correspond to the Calcite relations they are associated with. They are retained for now
    //   to enable users to migrate off of them.
    //   See:  https://github.com/substrait-io/substrait-java/issues/303
    final var builder =
        switch (set.getSetOp()) {
          case MINUS_PRIMARY -> relBuilder.minus(false, numInputs);
          case MINUS_PRIMARY_ALL, MINUS_MULTISET -> relBuilder.minus(true, numInputs);
          case INTERSECTION_PRIMARY, INTERSECTION_MULTISET -> relBuilder.intersect(
              false, numInputs);
          case INTERSECTION_MULTISET_ALL -> relBuilder.intersect(true, numInputs);
          case UNION_DISTINCT -> relBuilder.union(false, numInputs);
          case UNION_ALL -> relBuilder.union(true, numInputs);
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown set operation is not supported");
        };
    final RelNode node = builder.build();
    return applyRemap(node, set.getRemap());
  }

  @Override
  public RelNode visit(Aggregate aggregate, final Context context) throws RuntimeException {
    if (!PreCalciteAggregateValidator.isValidCalciteAggregate(aggregate)) {
      aggregate =
          PreCalciteAggregateValidator.PreCalciteAggregateTransformer
              .transformToValidCalciteAggregate(aggregate);
    }

    final RelNode child = aggregate.getInput().accept(this, context);
    final var groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter, context))
                        .collect(java.util.stream.Collectors.toList()))
            .collect(java.util.stream.Collectors.toList());
    final List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs, groupExprLists);

    final List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream()
            .map(measure -> fromMeasure(measure, context))
            .collect(java.util.stream.Collectors.toList());
    final RelNode node = relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
    return applyRemap(node, aggregate.getRemap());
  }

  private AggregateCall fromMeasure(final Aggregate.Measure measure, final Context context) {
    final var eArgs = measure.getFunction().arguments();
    final var arguments =
        IntStream.range(0, measure.getFunction().arguments().size())
            .mapToObj(
                i ->
                    eArgs
                        .get(i)
                        .accept(
                            measure.getFunction().declaration(),
                            i,
                            expressionRexConverter,
                            context))
            .collect(java.util.stream.Collectors.toList());
    final var operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(), measure.getFunction().outputType());
    if (!operator.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    final List<Integer> argIndex = new ArrayList<>();
    for (final RexNode arg : arguments) {
      // arguments are guaranteed to be RexInputRef because of the prior call to
      // transformToValidCalciteAggregate
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    final boolean distinct =
        measure.getFunction().invocation().equals(Expression.AggregationInvocation.DISTINCT);

    final SqlAggFunction aggFunction;
    final RelDataType returnType =
        typeConverter.toCalcite(typeFactory, measure.getFunction().getType());

    if (operator.get() instanceof SqlAggFunction) {
      aggFunction = (SqlAggFunction) operator.get();
    } else {
      final String msg =
          String.format(
              "Unable to convert non-aggregate operator: %s for substrait aggregate function %s",
              operator.get(), measure.getFunction().declaration().name());
      throw new IllegalArgumentException(msg);
    }

    int filterArg = -1;
    if (measure.getPreMeasureFilter().isPresent()) {
      final RexNode filter =
          measure.getPreMeasureFilter().get().accept(expressionRexConverter, context);
      filterArg = ((RexInputRef) filter).getIndex();
    }

    RelCollation relCollation = RelCollations.EMPTY;
    if (!measure.getFunction().sort().isEmpty()) {
      relCollation =
          RelCollations.of(
              measure.getFunction().sort().stream()
                  .map(sortField -> toRelFieldCollation(sortField, context))
                  .collect(Collectors.toList()));
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        Collections.emptyList(),
        argIndex,
        filterArg,
        null,
        relCollation,
        returnType,
        null);
  }

  @Override
  public RelNode visit(final Sort sort, final Context context) throws RuntimeException {
    final RelNode child = sort.getInput().accept(this, context);
    final List<RexNode> sortExpressions =
        sort.getSortFields().stream()
            .map(sortField -> directedRexNode(sortField, context))
            .collect(Collectors.toList());
    final RelNode node = relBuilder.push(child).sort(sortExpressions).build();
    return applyRemap(node, sort.getRemap());
  }

  private RexNode directedRexNode(final Expression.SortField sortField, final Context context) {
    final var expression = sortField.expr();
    final var rexNode = expression.accept(expressionRexConverter, context);
    final var sortDirection = sortField.direction();
    return switch (sortDirection) {
      case ASC_NULLS_FIRST -> relBuilder.nullsFirst(rexNode);
      case ASC_NULLS_LAST -> relBuilder.nullsLast(rexNode);
      case DESC_NULLS_FIRST -> relBuilder.nullsFirst(relBuilder.desc(rexNode));
      case DESC_NULLS_LAST -> relBuilder.nullsLast(relBuilder.desc(rexNode));
      case CLUSTERED -> throw new RuntimeException(
          String.format("Unexpected Expression.SortDirection: Clustered!"));
    };
  }

  @Override
  public RelNode visit(final Fetch fetch, final Context context) throws RuntimeException {
    final RelNode child = fetch.getInput().accept(this, context);
    final var optCount = fetch.getCount();
    final long count = optCount.orElse(-1L);
    final var offset = fetch.getOffset();
    if (offset > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("offset is overflowed as an integer: %d", offset));
    }
    if (count > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("count is overflowed as an integer: %d", count));
    }
    final RelNode node = relBuilder.push(child).limit((int) offset, (int) count).build();
    return applyRemap(node, fetch.getRemap());
  }

  private RelFieldCollation toRelFieldCollation(
      final Expression.SortField sortField, final Context context) {
    final var expression = sortField.expr();
    final var rex = expression.accept(expressionRexConverter, context);
    final var sortDirection = sortField.direction();
    final RexSlot rexSlot = (RexSlot) rex;
    final int fieldIndex = rexSlot.getIndex();
    var fieldDirection = RelFieldCollation.Direction.ASCENDING;
    var nullDirection = RelFieldCollation.NullDirection.UNSPECIFIED;
    switch (sortDirection) {
      case ASC_NULLS_FIRST -> nullDirection = RelFieldCollation.NullDirection.FIRST;
      case ASC_NULLS_LAST -> nullDirection = RelFieldCollation.NullDirection.LAST;
      case DESC_NULLS_FIRST -> {
        nullDirection = RelFieldCollation.NullDirection.FIRST;
        fieldDirection = RelFieldCollation.Direction.DESCENDING;
      }
      case DESC_NULLS_LAST -> {
        nullDirection = RelFieldCollation.NullDirection.LAST;
        fieldDirection = RelFieldCollation.Direction.DESCENDING;
      }
      case CLUSTERED -> fieldDirection = RelFieldCollation.Direction.CLUSTERED;

      default -> throw new RuntimeException(
          String.format("Unexpected Expression.SortDirection enum: %s !", sortDirection));
    }
    return new RelFieldCollation(fieldIndex, fieldDirection, nullDirection);
  }

  @Override
  public RelNode visitFallback(final Rel rel, final Context context) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  protected RelNode applyRemap(final RelNode relNode, final Optional<Rel.Remap> remap) {
    if (remap.isPresent()) {
      return applyRemap(relNode, remap.get());
    }
    return relNode;
  }

  private RelNode applyRemap(final RelNode relNode, final Rel.Remap remap) {
    final var rowType = relNode.getRowType();
    final var fieldNames = rowType.getFieldNames();
    final List<RexNode> rexList =
        remap.indices().stream()
            .map(
                index -> {
                  final RelDataTypeField t = rowType.getField(fieldNames.get(index), true, false);
                  return new RexInputRef(index, t.getValue());
                })
            .collect(java.util.stream.Collectors.toList());
    return relBuilder.push(relNode).project(rexList).build();
  }

  private void checkRexInputRefOnly(
      final RexNode rexNode, final String context, final String aggName) {
    if (!(rexNode instanceof RexInputRef)) {
      throw new UnsupportedOperationException(
          String.format(
              "Compound expression %s in %s of agg function %s is not implemented yet.",
              rexNode, context, aggName));
    }
  }

  public static class Context implements VisitationContext {
    public static Context newContext() {
      return new Context();
    }
  }
}
