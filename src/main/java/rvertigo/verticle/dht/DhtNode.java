package rvertigo.verticle.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.function.ReactiveLambda;
import rvertigo.verticle.ReactiveVertigo;
import rx.Observable;
import rx.Observer;
import rx.subjects.PublishSubject;

public class DhtNode<T extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final Integer myHash;
  protected Integer nextHash;

  protected AsyncMap<Integer, T> values = new AsyncMap<>(this);

  public DhtNode(Vertx vertx, String prefix, Integer myHash) {
    this.vertx = vertx;
    this.prefix = prefix;
    this.myHash = myHash;
    this.nextHash = myHash;
  }

  public DhtNode<T> join(RConsumer<DhtNode<T>> joined) {
    new Bootstrap<>(this).
      onSuccess(nextHash -> this.nextHash = nextHash).
      onSuccess(nextHash -> onBootstraped()).
      onSuccess(nextHash -> joined.accept(this)).
      bootstrap();

    return this;
  }

  protected void onBootstraped() {
    vertx.eventBus().consumer(DHT.toAddress(prefix, 0), (Message<byte[]> msg) -> processManagementMessage(msg));
    vertx.eventBus().consumer(DHT.toAddress(prefix, myHash), (Message<byte[]> msg) -> processManagementMessage(msg));
  }

  public Integer getIdentity() {
    return myHash;
  }

  public Integer getNext() {
    return this.nextHash;
  }

  public AsyncMap<Integer, T> getValues() {
    return this.values;
  }

  public Vertx getVertx() {
    return this.vertx;
  }

  protected void processManagementMessage(Message<byte[]> msg) {
    ReactiveLambda<Pair<DhtNode<T>, Message<byte[]>>, Message<byte[]>, Void> l = new ReactiveLambda<>(msg.body());
    l.context(new Pair<>(this, msg));
    l.onNext(msg);
  }

  public <R extends Serializable> void traverse(Integer start, Integer end, R identity,
    AsyncFunction<ReactiveLambda<DhtNode<T>, DhtNode<T>, R>, R> f,
    RConsumer<R> handler) {
    final Integer hash = myHash;

    byte[] ser = DHT.<T, R>managementMessage((pair, cb) -> {
      ReactiveLambda<Pair<DhtNode<T>, Message<byte[]>>, Message<byte[]>, R> c = pair;
      Message<byte[]> msg = pair.context().getValue1();

      if ((!start.equals(end) && DHT.isResponsible(start, end, c.context().getValue0().myHash))
        || DHT.isResponsible(c.context().getValue0(), start) || DHT.isResponsible(c.context().getValue0(), end)) {
        f.apply(new ReactiveLambda<>(f).context(c.context().getValue0()), (R result) -> {
          msg.reply(result);
        });
      }

      if (!hash.equals(c.context().getValue0().myHash)) {
        String addr = DHT.toAddress(c.context().getValue0().prefix, c.context().getValue0().nextHash);
        c.context().getValue0().vertx.eventBus().send(addr, c.serialize(), ar -> {
          if (ar.succeeded()) {
            msg.reply(ar.result().body());
          } else {
            msg.reply(ar.cause());
          }
        });
      } else {
        msg.reply(identity);
      }

    });

    vertx.eventBus().send(DHT.toAddress(prefix, nextHash), ser, (AsyncResult<Message<R>> ar) -> {
      if (ar.succeeded()) {
        handler.accept(ar.result().body());
      } else {
        handler.accept(null);
      }
    });
  }

  public void put(Integer key, T value, RConsumer<Boolean> callback) {
    traverse(key, key, Boolean.TRUE, (pair, v2) -> {
      pair.context().getValues().put(key, value);
      v2.accept(true);
    }, callback);
  }

  public void get(Integer key, RConsumer<T> callback) {
    traverse(key, key, null, (pair, cb) -> {
      T data = pair.context().getValues().get(key);
      cb.accept(data);
    }, callback);
  }

  public Observable<Map.Entry<Integer, T>> doRangeQuery(Integer from, Integer to) {
    PublishSubject<Map.Entry<Integer, T>> result = PublishSubject.<Map.Entry<Integer, T>>create();

    final String address = UUID.randomUUID().toString() + ".data";

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((Integer) o.getValue("k"), (T) o.getValue("v")));
    });

    traverse(from, to, Boolean.TRUE, (pair, v2) -> {
      DhtNode<T> node = pair.context();
      node.getValues().entrySet().stream().
        filter(e -> DHT.isResponsible(from, to, e.getKey())).
        forEach(e
          -> node.getVertx().eventBus().publish(address,
            new JsonObject().
            put("k", e.getKey()).
            put("v", e.getValue()))
        );
    }, reply -> {
      consumer.unregister();
      result.onCompleted();
    });

    return result;
  }

  @Override
  public String toString() {
    return Integer.toHexString(this.getIdentity()) + ": ["
      + Integer.toHexString(this.getIdentity()) + "-"
      + Integer.toHexString(this.getNext()) + "]";
  }
}
