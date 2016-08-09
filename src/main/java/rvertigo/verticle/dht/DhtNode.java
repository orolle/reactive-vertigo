package rvertigo.verticle.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.function.SerializableFunc2;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

public class DhtNode<KEY extends Serializable & Comparable<KEY>, VALUE extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final KEY myHash;
  protected KEY nextHash;

  public DhtNode(Vertx vertx, String prefix, KEY myHash) {
    this.vertx = vertx;
    this.prefix = prefix;
    this.myHash = myHash;
    this.nextHash = myHash;
  }

  public DhtNode<KEY, VALUE> join(RConsumer<DhtNode<KEY, VALUE>> joined) {
    bootstrap().
      doOnNext(nextHash -> this.nextHash = nextHash).
      doOnCompleted(() -> onBootstraped()).
      doOnCompleted(() -> joined.accept(this)).
      subscribe();

    return this;
  }

  public Observable<KEY> bootstrap() {
    final KEY hash = this.myHash;

    byte[] ser = DHT.managementMessage((lambda, cb) -> {
      DhtNode<KEY, VALUE> node = (DhtNode<KEY, VALUE>) lambda.node();
      Message<byte[]> msg = lambda.msg();

      if (DHT.isResponsible(node.myHash, node.nextHash, hash)) {
        msg.reply(node.nextHash);
        node.nextHash = hash;
      } else {
        node.vertx.eventBus().send(DHT.toAddress(node.prefix, node.nextHash), msg.body(),
          ar -> {
            if (ar.succeeded()) {
              msg.reply(ar.result().body());
            }
          });
      }

      cb.accept(null);
    });

    PublishSubject<KEY> result = PublishSubject.create();
    ReplaySubject<KEY> replay = ReplaySubject.create();
    result.subscribe(replay);

    this.vertx.eventBus().send(DHT.toAddress(this.prefix, 0),
      ser,
      new DeliveryOptions().setSendTimeout(10000),
      (AsyncResult<Message<KEY>> ar) -> {
        result.onNext(ar.succeeded() ? ar.result().body() : hash);
        result.onCompleted();
      });

    return replay;
  }

  protected void onBootstraped() {
    vertx.eventBus().consumer(DHT.toAddress(prefix, 0), (Message<byte[]> msg) -> processManagementMessage(msg));
    vertx.eventBus().consumer(DHT.toAddress(prefix, myHash), (Message<byte[]> msg) -> processManagementMessage(msg));
  }

  public KEY getIdentity() {
    return myHash;
  }

  public KEY getNext() {
    return this.nextHash;
  }

  public Vertx getVertx() {
    return this.vertx;
  }

  protected void processManagementMessage(Message<byte[]> msg) {
    DhtLambda<DhtNode<KEY, VALUE>, ? extends Serializable> l = new DhtLambda<>(msg.body());
    l.node(this);
    l.msg(msg);

    l.execute().
      subscribe();
  }

  public <NODE extends DhtNode<KEY, VALUE>, RESULT extends Serializable> void traverse(KEY start, KEY end,
    RESULT identity,
    SerializableFunc2<RESULT> resultReducer,
    AsyncFunction<DhtLambda<NODE, RESULT>, RESULT> f,
    RConsumer<AsyncResult<RESULT>> handler) {
    final KEY startHash = myHash;

    byte[] ser = DHT.<NODE, RESULT>managementMessage((lambda, cb) -> {
      final NODE node = lambda.node();
      final Message<byte[]> msg = lambda.msg();

      final PublishSubject<RESULT> result = PublishSubject.create();
      result.
        reduce(identity, resultReducer).
        subscribe(r -> {
          msg.reply(r);
        }, e -> {
        }, () -> {
        });

      AtomicLong counter = new AtomicLong(2);

      if ((!start.equals(end) && 
        DHT.isResponsible(start, end, node.myHash))
        || DHT.isResponsible(node.myHash, node.nextHash, start) || DHT.isResponsible(node.myHash, node.nextHash, end)) {

        f.apply(new DhtLambda<>(f).node(node).msg(msg), (RESULT r) -> {
          result.onNext(r);

          if (counter.decrementAndGet() == 0) {
            result.onCompleted();
          }
        });
      } else {
        if (counter.decrementAndGet() == 0) {
          result.onCompleted();
        }
      }

      if (!node.nextHash.equals(startHash)) {
        String addr = DHT.toAddress(node.prefix, node.nextHash);
        node.vertx.eventBus().<RESULT>send(addr, lambda.serialize(), ar -> {
          if (ar.succeeded()) {
            result.onNext(ar.result().body());
          } else {
            result.onError(ar.cause());
          }

          if (counter.decrementAndGet() == 0) {
            result.onCompleted();
          }
        });
      } else {
        if (counter.decrementAndGet() == 0) {
          result.onCompleted();
        }
      }
    });
    
    vertx.eventBus().send(DHT.toAddress(prefix, myHash), ser, (AsyncResult<Message<RESULT>> ar) -> {
      if (ar.succeeded()) {
        handler.accept(Future.succeededFuture(ar.result().body()));
      } else {
        handler.accept(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public String toString() {
    return this.getIdentity().toString() + ": ["
      + this.getIdentity().toString() + "-"
      + this.getNext().toString() + "]";
  }
}
