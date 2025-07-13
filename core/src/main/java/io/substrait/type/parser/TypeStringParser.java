package io.substrait.type.parser;

import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.SubstraitTypeLexer;
import io.substrait.type.SubstraitTypeParser;
import io.substrait.type.Type;
import java.util.function.BiFunction;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class TypeStringParser {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeStringParser.class);

  private TypeStringParser() {}

  public static Type parseSimple(final String str, final String namespace) {
    return parse(str, namespace, ParseToPojo::type);
  }

  public static ParameterizedType parseParameterized(final String str, final String namespace) {
    return parse(str, namespace, ParseToPojo::parameterizedType);
  }

  public static TypeExpression parseExpression(final String str, final String namespace) {
    return parse(str, namespace, ParseToPojo::typeExpression);
  }

  private static SubstraitTypeParser.StartContext parse(final String str) {
    final var lexer = new SubstraitTypeLexer(CharStreams.fromString(str));
    lexer.removeErrorListeners();
    lexer.addErrorListener(TypeErrorListener.INSTANCE);
    final var tokenStream = new CommonTokenStream(lexer);
    final var parser = new io.substrait.type.SubstraitTypeParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(TypeErrorListener.INSTANCE);
    return parser.start();
  }

  public static <T> T parse(
      final String str,
      final String namespace,
      final BiFunction<String, SubstraitTypeParser.StartContext, T> func) {
    return func.apply(namespace, parse(str));
  }

  public static TypeExpression parse(final String str, final ParseToPojo.Visitor visitor) {
    return parse(str).accept(visitor);
  }

  private static class TypeErrorListener extends BaseErrorListener {

    public static final TypeErrorListener INSTANCE = new TypeErrorListener();

    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String msg,
        final RecognitionException e) {
      throw new ParseError(msg, e, line, charPositionInLine);
    }
  }

  public static class ParseError extends RuntimeException {
    private final int line;
    private final int posInLine;

    public ParseError(
        final String message, final Throwable cause, final int line, final int posInLine) {
      super(message, cause);
      this.line = line;
      this.posInLine = posInLine;
    }
  }
}
