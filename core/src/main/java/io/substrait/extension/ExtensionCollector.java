package io.substrait.extension;

import com.github.bsideup.jabel.Desugar;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a mapping between function/type anchors and function/type references. Generates
 * references for new anchors as they are requested.
 *
 * <p>Used to replace instances of function and types in the POJOs with references when converting
 * from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan}
 */
public class ExtensionCollector extends AbstractExtensionLookup {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExtensionCollector.class);

  private final BidiMap<Integer, SimpleExtension.FunctionAnchor> funcMap;
  private final BidiMap<Integer, SimpleExtension.TypeAnchor> typeMap;
  private final BidiMap<Integer, String> uriMap;

  // start at 0 to make sure functionAnchors start with 1 according to spec
  private int counter = 0;

  public ExtensionCollector() {
    super(new HashMap<>(), new HashMap<>());
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
    uriMap = new BidiMap<>(new HashMap<>());
  }

  public int getFunctionReference(final SimpleExtension.Function declaration) {
    final Integer i = funcMap.reverseGet(declaration.getAnchor());
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    funcMap.put(counter, declaration.getAnchor());
    return counter;
  }

  public int getTypeReference(final SimpleExtension.TypeAnchor typeAnchor) {
    final Integer i = typeMap.reverseGet(typeAnchor);
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    typeMap.put(counter, typeAnchor);
    return counter;
  }

  public void addExtensionsToPlan(final Plan.Builder builder) {
    final SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUris(simpleExtensions.uris().values());
    builder.addAllExtensions(simpleExtensions.extensionList());
  }

  public void addExtensionsToExtendedExpression(final ExtendedExpression.Builder builder) {
    final SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUris(simpleExtensions.uris().values());
    builder.addAllExtensions(simpleExtensions.extensionList());
  }

  private SimpleExtensions getExtensions() {
    final var uriPos = new AtomicInteger(1);
    final var uris = new HashMap<String, SimpleExtensionURI>();

    final var extensionList = new ArrayList<SimpleExtensionDeclaration>();
    for (final var e : funcMap.forwardMap.entrySet()) {
      final SimpleExtensionURI uri =
          uris.computeIfAbsent(
              e.getValue().namespace(),
              k ->
                  SimpleExtensionURI.newBuilder()
                      .setExtensionUriAnchor(uriPos.getAndIncrement())
                      .setUri(k)
                      .build());
      final var decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionFunction(
                  SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                      .setFunctionAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUriReference(uri.getExtensionUriAnchor()))
              .build();
      extensionList.add(decl);
    }
    for (final var e : typeMap.forwardMap.entrySet()) {
      final SimpleExtensionURI uri =
          uris.computeIfAbsent(
              e.getValue().namespace(),
              k ->
                  SimpleExtensionURI.newBuilder()
                      .setExtensionUriAnchor(uriPos.getAndIncrement())
                      .setUri(k)
                      .build());
      final var decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionType(
                  SimpleExtensionDeclaration.ExtensionType.newBuilder()
                      .setTypeAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUriReference(uri.getExtensionUriAnchor()))
              .build();
      extensionList.add(decl);
    }
    return new SimpleExtensions(uris, extensionList);
  }

  @Desugar
  private record SimpleExtensions(
      HashMap<String, SimpleExtensionURI> uris,
      ArrayList<SimpleExtensionDeclaration> extensionList) {}

  /** We don't depend on guava... */
  private static class BidiMap<T1, T2> {
    private final Map<T1, T2> forwardMap;
    private final Map<T2, T1> reverseMap;

    public BidiMap(final Map<T1, T2> forwardMap) {
      this.forwardMap = forwardMap;
      this.reverseMap = new HashMap<>();
    }

    public T2 get(final T1 t1) {
      return forwardMap.get(t1);
    }

    public T1 reverseGet(final T2 t2) {
      return reverseMap.get(t2);
    }

    public void put(final T1 t1, final T2 t2) {
      forwardMap.put(t1, t2);
      reverseMap.put(t2, t1);
    }
  }
}
