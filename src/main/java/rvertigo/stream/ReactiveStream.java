package rvertigo.stream;

import java.util.function.Function;
import java.util.stream.Stream;

import rvertigo.function.AsyncFunction;

public interface ReactiveStream<T> extends Stream<T> {

  @Override
  public <R> ReactiveStream<R> map(Function<? super T, ? extends R> mapper);

  public <R> ReactiveStream<R> map(AsyncFunction<? super T, ? extends R> mapper);

  /**
   * Distributed queue
   * 
   * @return Distribured queue
   */
  public ReactiveStream<T> queue();
  
}
