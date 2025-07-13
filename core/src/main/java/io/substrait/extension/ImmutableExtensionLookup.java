package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains a mapping between function anchors and function references. Generates references for
 * new anchors.
 */
public class ImmutableExtensionLookup extends AbstractExtensionLookup {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ImmutableExtensionLookup.class);

  private final int counter = -1;

  private ImmutableExtensionLookup(
      final Map<Integer, SimpleExtension.FunctionAnchor> functionMap,
      final Map<Integer, SimpleExtension.TypeAnchor> typeMap) {
    super(functionMap, typeMap);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Integer, SimpleExtension.FunctionAnchor> functionMap = new HashMap<>();
    private final Map<Integer, SimpleExtension.TypeAnchor> typeMap = new HashMap<>();

    public Builder from(final Plan plan) {
      return from(plan.getExtensionUrisList(), plan.getExtensionsList());
    }

    public Builder from(final ExtendedExpression extendedExpression) {
      return from(
          extendedExpression.getExtensionUrisList(), extendedExpression.getExtensionsList());
    }

    private Builder from(
        final List<SimpleExtensionURI> simpleExtensionURIs,
        final List<SimpleExtensionDeclaration> simpleExtensionDeclarations) {
      final Map<Integer, String> namespaceMap = new HashMap<>();
      for (final var extension : simpleExtensionURIs) {
        namespaceMap.put(extension.getExtensionUriAnchor(), extension.getUri());
      }

      // Add all functions used in plan to the functionMap
      for (final var extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionFunction()) {
          continue;
        }
        final SimpleExtensionDeclaration.ExtensionFunction func = extension.getExtensionFunction();
        final int reference = func.getFunctionAnchor();
        final String namespace = namespaceMap.get(func.getExtensionUriReference());
        if (namespace == null) {
          throw new IllegalStateException(
              "Could not find extension URI of " + func.getExtensionUriReference());
        }
        final String name = func.getName();
        final SimpleExtension.FunctionAnchor anchor =
            SimpleExtension.FunctionAnchor.of(namespace, name);
        functionMap.put(reference, anchor);
      }

      // Add all types used in plan to the typeMap
      for (final var extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionType()) {
          continue;
        }
        final SimpleExtensionDeclaration.ExtensionType type = extension.getExtensionType();
        final int reference = type.getTypeAnchor();
        final String namespace = namespaceMap.get(type.getExtensionUriReference());
        if (namespace == null) {
          throw new IllegalStateException(
              "Could not find extension URI of " + type.getExtensionUriReference());
        }
        final String name = type.getName();
        final SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(namespace, name);
        typeMap.put(reference, anchor);
      }

      return this;
    }

    public ImmutableExtensionLookup build() {
      return new ImmutableExtensionLookup(
          Collections.unmodifiableMap(functionMap), Collections.unmodifiableMap(typeMap));
    }
  }
}
