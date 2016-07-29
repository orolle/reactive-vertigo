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
import rvertigo.verticle.ReactiveVertigo;
import rx.Completable;
import rx.Observable;
import rx.Observer;
import rx.Single;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

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
    // System.out.println("register node " + Integer.toHexString(myHash) + " to " + DHT.toAddress(prefix, 0));
    // System.out.println("register node " + Integer.toHexString(myHash)  + " to " + DHT.toAddress(prefix, myHash));
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
    DhtLambda<DhtNode<T>, Void> l = new DhtLambda<>(msg.body());
    l.contextNode(this);
    l.contextMsg(msg);
    l.onNext(null);
  }

  public <R extends Serializable> void traverse(Integer start, Integer end, R identity,
    AsyncFunction<DhtLambda<DhtNode<T>, R>, R> f,
    RConsumer<R> handler) {
    final Integer hash = myHash;

    byte[] ser = DHT.<T, R>managementMessage((lambda, cb) -> {
      final DhtNode<T> node = lambda.node();
      final Message<byte[]> msg = lambda.msg();

      if ((!start.equals(end) && DHT.isResponsible(start, end, node.myHash))
        || DHT.isResponsible(node, start) || DHT.isResponsible(node, end)) {
        f.apply(new DhtLambda<>(f).contextNode(node).contextMsg(msg), (R result) -> {
          msg.reply(result);
        });
      }

      if (!hash.equals(node.myHash)) {
        String addr = DHT.toAddress(node.prefix, node.nextHash);
        node.vertx.eventBus().send(addr, lambda.serialize(), ar -> {
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

    // System.out.println("send node " + Integer.toHexString(myHash)  + " to " + DHT.toAddress(prefix, nextHash));
    vertx.eventBus().send(DHT.toAddress(prefix, nextHash), ser, (AsyncResult<Message<R>> ar) -> {
      if (ar.succeeded()) {
        handler.accept(ar.result().body());
      } else {
        ar.cause().printStackTrace();
        handler.accept(null);
      }
    });
  }

  /*
  public <R extends Serializable> Observable<R> traverse(Integer start, Integer end, R identity,
    AsyncFunction<ReactiveLambda<DhtNode<T>, DhtNode<T>, R>, R> f) {
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
    
    PublishSubject<R> result = PublishSubject.create();
    ReplaySubject<R> replay = ReplaySubject.create();
    result.subscribe(replay);

    vertx.eventBus().send(DHT.toAddress(prefix, nextHash), ser, (AsyncResult<Message<R>> ar) -> {
      if (ar.succeeded()) {
        result.onNext(ar.result().body());
      } else {
        result.onError(ar.cause());
      }
    });
    
    return replay;
  }
   */
  public Completable put(Integer key, T value) {
    PublishSubject<Void> result = PublishSubject.<Void>create();

    traverse(key, key, true, (lambda, v2) -> {
      lambda.node().getValues().put(key, value);
      v2.accept(true);
    }, b -> {
      if (b) {
        result.onCompleted();
      } else {
        result.onError(new RuntimeException("Something went wrong during put(" + key.toString() + ", "
          + value.toString() + ")"));
      }
    });

    return Completable.fromObservable(result);
  }

  public Observable<T> get(Integer key) {
    PublishSubject<T> result = PublishSubject.create();

    ReplaySubject<T> replay = ReplaySubject.create();
    result.subscribe(replay);

    traverse(key, key, null, (pair, cb) -> {
      T data = pair.node().getValues().get(key);
      cb.accept(data);
    }, (T data) -> {
      if(data != null) {
        result.onNext(data);
      }
      
      result.onCompleted();
    });

    return replay;
  }

  public Observable<Map.Entry<Integer, T>> rangeQuery(Integer from, Integer to) {
    PublishSubject<Map.Entry<Integer, T>> result = PublishSubject.<Map.Entry<Integer, T>>create();

    ReplaySubject<Map.Entry<Integer, T>> replay = ReplaySubject.create();
    result.subscribe(replay);

    final String address = UUID.randomUUID().toString() + ".data";

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((Integer) o.getValue("k"), (T) o.getValue("v")));
    });

    traverse(from, to, Boolean.TRUE, (pair, v2) -> {
      DhtNode<T> node = pair.node();
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

    return replay;
  }

  @Override
  public String toString() {
    return Integer.toHexString(this.getIdentity()) + ": ["
      + Integer.toHexString(this.getIdentity()) + "-"
      + Integer.toHexString(this.getNext()) + "]";
  }
}
