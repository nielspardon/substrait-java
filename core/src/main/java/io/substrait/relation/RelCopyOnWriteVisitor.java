package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.or;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Class used to visit all child relations from a root relation and optionally replace subtrees by
 * overriding a visitor method. The traversal will include relations inside of subquery expressions.
 * By default, no subtree substitution will be performed. However, if a visit method is overridden
 * to return a non-empty optional value, then that value will replace the relation in the tree.
 */
public class RelCopyOnWriteVisitor<E extends Exception>
    implements RelVisitor<Optional<Rel>, EmptyVisitationContext, E> {

  private final ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor;

  public RelCopyOnWriteVisitor() {
    this.expressionCopyOnWriteVisitor = new ExpressionCopyOnWriteVisitor<>(this);
  }

  public RelCopyOnWriteVisitor(final ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor) {
    this.expressionCopyOnWriteVisitor = expressionCopyOnWriteVisitor;
  }

  public RelCopyOnWriteVisitor(
      final Function<RelCopyOnWriteVisitor<E>, ExpressionCopyOnWriteVisitor<E>> fn) {
    this.expressionCopyOnWriteVisitor = fn.apply(this);
  }

  protected ExpressionCopyOnWriteVisitor<E> getExpressionCopyOnWriteVisitor() {
    return expressionCopyOnWriteVisitor;
  }

  @Override
  public Optional<Rel> visit(final Aggregate aggregate, final EmptyVisitationContext context)
      throws E {
    final var input = aggregate.getInput().accept(this, context);
    final var groupings = transformList(aggregate.getGroupings(), context, this::visitGrouping);
    final var measures = transformList(aggregate.getMeasures(), context, this::visitMeasure);

    if (allEmpty(input, groupings, measures)) {
      return Optional.empty();
    }
    return Optional.of(
        Aggregate.builder()
            .from(aggregate)
            .input(input.orElse(aggregate.getInput()))
            .groupings(groupings.orElse(aggregate.getGroupings()))
            .measures(measures.orElse(aggregate.getMeasures()))
            .build());
  }

  protected Optional<Aggregate.Grouping> visitGrouping(
      final Aggregate.Grouping grouping, final EmptyVisitationContext context) throws E {
    return visitExprList(grouping.getExpressions(), context)
        .map(exprs -> Aggregate.Grouping.builder().from(grouping).expressions(exprs).build());
  }

  protected Optional<Aggregate.Measure> visitMeasure(
      final Aggregate.Measure measure, final EmptyVisitationContext context) throws E {
    final var preMeasureFilter = visitOptionalExpression(measure.getPreMeasureFilter(), context);
    final var afi = visitAggregateFunction(measure.getFunction(), context);

    if (allEmpty(preMeasureFilter, afi)) {
      return Optional.empty();
    }
    return Optional.of(
        Aggregate.Measure.builder()
            .from(measure)
            .preMeasureFilter(or(preMeasureFilter, measure::getPreMeasureFilter))
            .function(afi.orElse(measure.getFunction()))
            .build());
  }

  protected Optional<AggregateFunctionInvocation> visitAggregateFunction(
      final AggregateFunctionInvocation afi, final EmptyVisitationContext context) throws E {
    final var arguments = visitFunctionArguments(afi.arguments(), context);
    final var sort = transformList(afi.sort(), context, this::visitSortField);

    if (allEmpty(arguments, sort)) {
      return Optional.empty();
    }
    return Optional.of(
        AggregateFunctionInvocation.builder()
            .from(afi)
            .arguments(arguments.orElse(afi.arguments()))
            .sort(sort.orElse(afi.sort()))
            .build());
  }

  @Override
  public Optional<Rel> visit(final EmptyScan emptyScan, final EmptyVisitationContext context)
      throws E {
    final Optional<Expression> filter = visitOptionalExpression(emptyScan.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        EmptyScan.builder()
            .from(emptyScan)
            .filter(filter.isPresent() ? filter : emptyScan.getFilter())
            .build());
  }

  @Override
  public Optional<Rel> visit(final Fetch fetch, final EmptyVisitationContext context) throws E {
    return fetch
        .getInput()
        .accept(this, context)
        .map(input -> Fetch.builder().from(fetch).input(input).build());
  }

  @Override
  public Optional<Rel> visit(final Filter filter, final EmptyVisitationContext context) throws E {
    final var input = filter.getInput().accept(this, context);
    final var condition = filter.getCondition().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(input, condition)) {
      return Optional.empty();
    }
    return Optional.of(
        Filter.builder()
            .from(filter)
            .input(input.orElse(filter.getInput()))
            .condition(condition.orElse(filter.getCondition()))
            .build());
  }

  @Override
  public Optional<Rel> visit(final Join join, final EmptyVisitationContext context) throws E {
    final var left = join.getLeft().accept(this, context);
    final var right = join.getRight().accept(this, context);
    final var condition = visitOptionalExpression(join.getCondition(), context);
    final var postFilter = visitOptionalExpression(join.getPostJoinFilter(), context);

    if (allEmpty(left, right, condition, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableJoin.builder()
            .from(join)
            .left(left.orElse(join.getLeft()))
            .right(right.orElse(join.getRight()))
            .condition(or(condition, join::getCondition))
            .postJoinFilter(or(postFilter, join::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(final Set set, final EmptyVisitationContext context) throws E {
    return transformList(set.getInputs(), context, (t, c) -> t.accept(this, c))
        .map(s -> Set.builder().from(set).inputs(s).build());
  }

  @Override
  public Optional<Rel> visit(final NamedScan namedScan, final EmptyVisitationContext context)
      throws E {
    final var filter = visitOptionalExpression(namedScan.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        NamedScan.builder().from(namedScan).filter(or(filter, namedScan::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(final LocalFiles localFiles, final EmptyVisitationContext context)
      throws E {
    final var filter = visitOptionalExpression(localFiles.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        LocalFiles.builder().from(localFiles).filter(or(filter, localFiles::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(final Project project, final EmptyVisitationContext context) throws E {
    final var input = project.getInput().accept(this, context);
    final var expressions = visitExprList(project.getExpressions(), context);

    if (allEmpty(input, expressions)) {
      return Optional.empty();
    }
    return Optional.of(
        Project.builder()
            .from(project)
            .input(input.orElse(project.getInput()))
            .expressions(expressions.orElse(project.getExpressions()))
            .build());
  }

  @Override
  public Optional<Rel> visit(final Expand expand, final EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final NamedWrite write, final EmptyVisitationContext context)
      throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final ExtensionWrite write, final EmptyVisitationContext context)
      throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final NamedDdl ddl, final EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final ExtensionDdl ddl, final EmptyVisitationContext context)
      throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final NamedUpdate update, final EmptyVisitationContext context)
      throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(final Sort sort, final EmptyVisitationContext context) throws E {
    final var input = sort.getInput().accept(this, context);
    final var sortFields = transformList(sort.getSortFields(), context, this::visitSortField);

    if (allEmpty(input, sortFields)) {
      return Optional.empty();
    }
    return Optional.of(
        Sort.builder()
            .from(sort)
            .input(input.orElse(sort.getInput()))
            .sortFields(sortFields.orElse(sort.getSortFields()))
            .build());
  }

  @Override
  public Optional<Rel> visit(final Cross cross, final EmptyVisitationContext context) throws E {
    final var left = cross.getLeft().accept(this, context);
    final var right = cross.getRight().accept(this, context);

    if (allEmpty(left, right)) {
      return Optional.empty();
    }
    return Optional.of(
        Cross.builder()
            .from(cross)
            .left(left.orElse(cross.getLeft()))
            .right(right.orElse(cross.getRight()))
            .build());
  }

  @Override
  public Optional<Rel> visit(
      final VirtualTableScan virtualTableScan, final EmptyVisitationContext context) throws E {
    final var filter = visitOptionalExpression(virtualTableScan.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        VirtualTableScan.builder()
            .from(virtualTableScan)
            .filter(or(filter, virtualTableScan::getFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(
      final ExtensionLeaf extensionLeaf, final EmptyVisitationContext context) throws E {
    return Optional.empty();
  }

  @Override
  public Optional<Rel> visit(
      final ExtensionSingle extensionSingle, final EmptyVisitationContext context) throws E {
    return extensionSingle
        .getInput()
        .accept(this, context)
        .map(input -> ExtensionSingle.builder().from(extensionSingle).input(input).build());
  }

  @Override
  public Optional<Rel> visit(
      final ExtensionMulti extensionMulti, final EmptyVisitationContext context) throws E {
    return transformList(extensionMulti.getInputs(), context, (rel, c) -> rel.accept(this, c))
        .map(inputs -> ExtensionMulti.builder().from(extensionMulti).inputs(inputs).build());
  }

  @Override
  public Optional<Rel> visit(
      final ExtensionTable extensionTable, final EmptyVisitationContext context) throws E {
    final var filter = visitOptionalExpression(extensionTable.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        ExtensionTable.builder()
            .from(extensionTable)
            .filter(or(filter, extensionTable::getFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(final HashJoin hashJoin, final EmptyVisitationContext context)
      throws E {
    final var left = hashJoin.getLeft().accept(this, context);
    final var right = hashJoin.getRight().accept(this, context);
    final var leftKeys = transformList(hashJoin.getLeftKeys(), context, this::visitFieldReference);
    final var rightKeys =
        transformList(hashJoin.getRightKeys(), context, this::visitFieldReference);
    final var postFilter = visitOptionalExpression(hashJoin.getPostJoinFilter(), context);

    if (allEmpty(left, right, leftKeys, rightKeys, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        HashJoin.builder()
            .from(hashJoin)
            .left(left.orElse(hashJoin.getLeft()))
            .right(right.orElse(hashJoin.getRight()))
            .leftKeys(leftKeys.orElse(hashJoin.getLeftKeys()))
            .rightKeys(rightKeys.orElse(hashJoin.getRightKeys()))
            .postJoinFilter(or(postFilter, hashJoin::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(final MergeJoin mergeJoin, final EmptyVisitationContext context)
      throws E {
    final var left = mergeJoin.getLeft().accept(this, context);
    final var right = mergeJoin.getRight().accept(this, context);
    final var leftKeys = transformList(mergeJoin.getLeftKeys(), context, this::visitFieldReference);
    final var rightKeys =
        transformList(mergeJoin.getRightKeys(), context, this::visitFieldReference);
    final var postFilter = visitOptionalExpression(mergeJoin.getPostJoinFilter(), context);

    if (allEmpty(left, right, leftKeys, rightKeys, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        MergeJoin.builder()
            .from(mergeJoin)
            .left(left.orElse(mergeJoin.getLeft()))
            .right(right.orElse(mergeJoin.getRight()))
            .leftKeys(leftKeys.orElse(mergeJoin.getLeftKeys()))
            .rightKeys(rightKeys.orElse(mergeJoin.getRightKeys()))
            .postJoinFilter(or(postFilter, mergeJoin::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(
      final NestedLoopJoin nestedLoopJoin, final EmptyVisitationContext context) throws E {
    final var left = nestedLoopJoin.getLeft().accept(this, context);
    final var right = nestedLoopJoin.getRight().accept(this, context);
    final var condition =
        nestedLoopJoin.getCondition().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(left, right, condition)) {
      return Optional.empty();
    }
    return Optional.of(
        NestedLoopJoin.builder()
            .from(nestedLoopJoin)
            .left(left.orElse(nestedLoopJoin.getLeft()))
            .right(right.orElse(nestedLoopJoin.getRight()))
            .condition(condition.orElse(nestedLoopJoin.getCondition()))
            .build());
  }

  @Override
  public Optional<Rel> visit(
      final ConsistentPartitionWindow consistentPartitionWindow,
      final EmptyVisitationContext context)
      throws E {
    final var windowFunctions =
        transformList(
            consistentPartitionWindow.getWindowFunctions(), context, this::visitWindowRelFunction);
    final var partitionExpressions =
        transformList(
            consistentPartitionWindow.getPartitionExpressions(),
            context,
            (t, c) -> t.accept(getExpressionCopyOnWriteVisitor(), c));
    final var sorts =
        transformList(consistentPartitionWindow.getSorts(), context, this::visitSortField);

    if (allEmpty(windowFunctions, partitionExpressions, sorts)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.builder()
            .from(consistentPartitionWindow)
            .partitionExpressions(
                partitionExpressions.orElse(consistentPartitionWindow.getPartitionExpressions()))
            .sorts(sorts.orElse(consistentPartitionWindow.getSorts()))
            .windowFunctions(windowFunctions.orElse(consistentPartitionWindow.getWindowFunctions()))
            .build());
  }

  protected Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> visitWindowRelFunction(
      final ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunctionInvocation,
      final EmptyVisitationContext context)
      throws E {
    final var functionArgs =
        visitFunctionArguments(windowRelFunctionInvocation.arguments(), context);

    if (allEmpty(functionArgs)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
            .from(windowRelFunctionInvocation)
            .arguments(functionArgs.orElse(windowRelFunctionInvocation.arguments()))
            .build());
  }

  // utilities

  protected Optional<List<Expression>> visitExprList(
      final List<Expression> exprs, final EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (t, c) -> t.accept(getExpressionCopyOnWriteVisitor(), c));
  }

  public Optional<FieldReference> visitFieldReference(
      final FieldReference fieldReference, final EmptyVisitationContext context) throws E {
    final var inputExpression = visitOptionalExpression(fieldReference.inputExpression(), context);
    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }

    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  protected Optional<List<FunctionArg>> visitFunctionArguments(
      final List<FunctionArg> funcArgs, final EmptyVisitationContext context) throws E {
    return CopyOnWriteUtils.<FunctionArg, EmptyVisitationContext, E>transformList(
        funcArgs,
        context,
        (arg, c) -> {
          if (arg instanceof final Expression expr) {
            return expr.accept(getExpressionCopyOnWriteVisitor(), c)
                .flatMap(Optional::<FunctionArg>of);
          } else {
            return Optional.empty();
          }
        });
  }

  protected Optional<Expression.SortField> visitSortField(
      final Expression.SortField sortField, final EmptyVisitationContext context) throws E {
    return sortField
        .expr()
        .accept(getExpressionCopyOnWriteVisitor(), context)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }

  private Optional<Expression> visitOptionalExpression(
      final Optional<Expression> optExpr, final EmptyVisitationContext context) throws E {
    // not using optExpr.map to allow us to propagate the THROWABLE nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(getExpressionCopyOnWriteVisitor(), context);
    }
    return Optional.empty();
  }
}
