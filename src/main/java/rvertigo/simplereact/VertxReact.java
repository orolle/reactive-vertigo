package rvertigo.simplereact;

import io.vertx.core.Vertx;

import com.aol.simple.react.stream.lazy.LazyReact;
import java.io.Serializable;
import java.util.concurrent.ForkJoinPool;

public class VertxReact {
  protected final Vertx vertx;
  protected final LazyReact react;

  public VertxReact(Vertx vertx) {
    super();
    this.vertx = vertx;
//    this.react = new LazyReact(new VertxExecutor(vertx));
    this.react = new LazyReact(new ForkJoinPool(1));
  }
  
  public LazyReact getReact() {
    return this.react;
  }
  
  public <R extends Serializable> ReactPipeline<R> fromEventbus(String address) {
    return new ReactPipeline<R>().fromEventbus(address);
  }
}
