package microservicerx.function;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface AsyncFunction<T, R> extends Serializable {
  /**
   * Applies this function to the given argument.
   *
   * @param t
   *          the function argument
   * @return the function result
   */
  public void apply(T arg, RConsumer<R> callback);

  /**
   * Returns a composed function that first applies the {@code before} function
   * to its input, and then applies this function to the result. If evaluation
   * of either function throws an exception, it is relayed to the caller of the
   * composed function.
   *
   * @param <V>
   *          the type of input to the {@code before} function, and to the
   *          composed function
   * @param before
   *          the function to apply before this function is applied
   * @return a composed function that first applies the {@code before} function
   *         and then applies this function
   * @throws NullPointerException
   *           if before is null
   *
   * @see #andThen(Function)
   */
  default <V> AsyncFunction<V, R> compose(AsyncFunction<? super V, ? extends T> before) {
    Objects.requireNonNull(before);
    return (V v, RConsumer<R> cb) -> before.apply(v, r -> apply(r, cb));
  }

  /**
   * Returns a composed function that first applies this function to its input,
   * and then applies the {@code after} function to the result. If evaluation of
   * either function throws an exception, it is relayed to the caller of the
   * composed function.
   *
   * @param <V>
   *          the type of output of the {@code after} function, and of the
   *          composed function
   * @param after
   *          the function to apply after this function is applied
   * @return a composed function that first applies this function and then
   *         applies the {@code after} function
   * @throws NullPointerException
   *           if after is null
   *
   * @see #compose(Function)
   */
  default <V> AsyncFunction<T, V> andThen(AsyncFunction<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return (T t, RConsumer<V> cb) -> apply(t, r -> after.apply(r, v -> cb.accept(v)));
  }

  /**
   * Returns a function that always returns its input argument.
   *
   * @param <T>
   *          the type of the input and output objects to the function
   * @return a function that always returns its input argument
   */
  static <T> AsyncFunction<T, T> identity() {
    return (t, cb) -> cb.accept(t);
  }
}