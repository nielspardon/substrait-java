package io.substrait.plan;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Version;
import io.substrait.relation.RelProtoConverter;
import java.util.ArrayList;
import java.util.List;

/** Converts from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan} */
public class PlanProtoConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PlanProtoConverter.class);

  public Plan toProto(final io.substrait.plan.Plan plan) {
    final List<PlanRel> planRels = new ArrayList<>();
    final ExtensionCollector functionCollector = new ExtensionCollector();
    for (final io.substrait.plan.Plan.Root root : plan.getRoots()) {
      final Rel input = new RelProtoConverter(functionCollector).toProto(root.getInput());
      planRels.add(
          PlanRel.newBuilder()
              .setRoot(
                  io.substrait.proto.RelRoot.newBuilder()
                      .setInput(input)
                      .addAllNames(root.getNames()))
              .build());
    }
    final Plan.Builder builder =
        Plan.newBuilder()
            .addAllRelations(planRels)
            .addAllExpectedTypeUrls(plan.getExpectedTypeUrls());
    functionCollector.addExtensionsToPlan(builder);
    if (plan.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(plan.getAdvancedExtension().get());
    }

    final Version.Builder versionBuilder =
        Version.newBuilder()
            .setMajorNumber(plan.getVersion().getMajor())
            .setMinorNumber(plan.getVersion().getMinor())
            .setPatchNumber(plan.getVersion().getPatch());

    plan.getVersion().getGitHash().ifPresent(gh -> versionBuilder.setGitHash(gh));
    plan.getVersion().getProducer().ifPresent(p -> versionBuilder.setProducer(p));

    builder.setVersion(versionBuilder);

    return builder.build();
  }
}
