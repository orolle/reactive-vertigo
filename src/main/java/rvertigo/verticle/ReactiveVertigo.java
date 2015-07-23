package rvertigo.verticle;

import io.vertx.core.Vertx;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import org.javatuples.Pair;

import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.function.ReactiveLambda;
import rvertigo.stream.ReactivePipeline;
import rvertigo.stream.ReactiveStream;
import rvertigo.verticle.dht.DhtNode;

public class ReactiveVertigo {
  protected final DhtNode<String> node;
  protected final Vertx vertx;

  private static final Random rand = new Random(10);

  public ReactiveVertigo(Vertx vertx) {
    this.vertx = vertx;
    this.node = new DhtNode<String>(vertx, "DhtNode", random());
  }

  public ReactiveVertigo onJoined(RConsumer<ReactiveVertigo> f) {
    this.node.join(node -> f.accept(this));
    return this;
  }

  public <R extends Serializable> void traverse(Integer start, Integer end, R identity,
    AsyncFunction<Pair<ReactiveLambda<Void, DhtNode<String>, R>, DhtNode<String>>, R> f,
    RConsumer<R> handler) {
    this.node.traverse(start, end, identity, f, handler);
  }

  public void put(Integer key, String value, RConsumer<Boolean> callback) {
    this.node.put(key, value, callback);
  }

  public void get(Integer key, RConsumer<String> callback) {
    this.node.get(key, callback);
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
}
