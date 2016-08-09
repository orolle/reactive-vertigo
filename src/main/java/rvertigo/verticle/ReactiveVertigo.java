package rvertigo.verticle;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import org.javatuples.Pair;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.verticle.dht.DhtLambda;
import rvertigo.stream.ReactivePipeline;
import rvertigo.stream.ReactiveStream;
import rvertigo.verticle.dht.DhtMap;
import rvertigo.verticle.dht.DhtNode;
import rx.Completable;
import rx.Observable;
import rx.Single;

public class ReactiveVertigo<V extends Serializable> {
  protected final DhtMap<Integer, V> node;
  //protected DhtNode<LazyReact> noder;
  protected final Vertx vertx;

  private static final Random rand = new Random(10);

  public ReactiveVertigo(Vertx vertx) {
    this.vertx = vertx;
    this.node = new DhtMap<Integer, V>(vertx, "DhtNode", random());
  }

  public ReactiveVertigo onJoined(RConsumer<ReactiveVertigo> f) {
    this.node.join(node -> f.accept(this));
    return this;
  }
  public Completable put(Integer key, V value) {
    return this.node.put(key, value);
  }

  public Observable<V> get(Integer key) {
    return this.node.get(key);
  }
  
  public Observable<Map.Entry<Integer, V>> rangeQuery(Integer from, Integer to) {
    return this.node.rangeQuery(from, to);
  }

  private Integer random() {
    return rand.nextBoolean() ? rand.nextInt(Integer.MAX_VALUE) : -rand.nextInt(Integer.MAX_VALUE);
  }

  public <T> ReactiveStream<T> fromEventbus(String... address) {
    ReactivePipeline.Source<T> r = new ReactivePipeline.Source<>(vertx);
    Arrays.asList(address).forEach(str -> {
      vertx.eventBus().<T> consumer(str).handler(msg -> r.callback().accept(msg.body()));
    });
    return r;
  }
  
  public DhtNode<Integer, V> getDhtNode() {
    return this.node;
  }
}
