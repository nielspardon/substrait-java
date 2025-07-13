package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.isthmus.sql.SubstraitSqlValidator;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Plan;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public SqlToSubstrait() {
    this(null);
  }

  public SqlToSubstrait(final FeatureBoard features) {
    super(features);
  }

  public Plan execute(final String sql, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return executeInner(sql, validator, catalogReader);
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(final String sql, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return sqlToRelNode(sql, validator, catalogReader);
  }

  private Plan executeInner(
      final String sql, final SqlValidator validator, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final var builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
    sqlToRelNode(sql, validator, catalogReader).stream()
        .map(root -> SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION, featureBoard))
        .forEach(root -> builder.addRoots(root));

    final PlanProtoConverter planToProto = new PlanProtoConverter();

    return planToProto.toProto(builder.build());
  }

  private List<RelRoot> sqlToRelNode(
      final String sql, final SqlValidator validator, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final var parsedList = parser.parseStmtList();
    final SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    final List<RelRoot> roots =
        parsedList.stream()
            .map(parsed -> getBestExpRelRoot(converter, parsed))
            .collect(java.util.stream.Collectors.toList());
    return roots;
  }

  @VisibleForTesting
  SqlToRelConverter createSqlToRelConverter(
      final SqlValidator validator, final Prepare.CatalogReader catalogReader) {
    final SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    return converter;
  }

  @VisibleForTesting
  static RelRoot getBestExpRelRoot(final SqlToRelConverter converter, final SqlNode parsed) {
    RelRoot root = converter.convertQuery(parsed, true, true);
    {
      final var program = HepProgram.builder().build();
      final HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(root.rel);
      root = root.withRel(hepPlanner.findBestExp());
    }
    return root;
  }
}
