package rvertigo.verticle.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.rxjava.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.function.SerializableFunc2;
import rx.Observable;
import rx.Single;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

public class DhtNode<KEY extends Serializable & Comparable<KEY>, VALUE extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final KEY myHash;
  protected KEY nextHash;
  
  protected final DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(10000);
  
  protected MessageConsumer<byte[]> broadcastConsumer;
  protected MessageConsumer<byte[]> nodeConsumer;

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
      DhtNode<KEY, VALUE> node = lambda.node();
      Message<byte[]> msg = lambda.msg();

      if (DHT.isResponsible(node.myHash, node.nextHash, hash)) {
        msg.reply(node.nextHash);
        node.nextHash = hash;
      } else {
        node.vertx.eventBus().
          <KEY>sendObservable(DHT.toAddress(node.prefix, node.nextHash), msg.body(), node.deliveryOptions).
          subscribe(reply -> msg.reply(reply.body()));
      }

      cb.accept(null);
    });
    
    return this.vertx.eventBus().
      <KEY>sendObservable(DHT.toAddress(this.prefix, 0), ser, deliveryOptions).
      map(msg -> msg.body()).
      onErrorResumeNext(e -> Observable.just(hash));
  }

  protected void onBootstraped() {
    Action1<Throwable> processException = e -> {
      e.printStackTrace();
    };
    Action0 processCompleted = () -> {
    };

    broadcastConsumer = vertx.eventBus().consumer(DHT.toAddress(prefix, 0));
    broadcastConsumer.toObservable().
      subscribe(
        msg -> processManagementMessage(msg),
        processException,
        processCompleted);

    nodeConsumer = vertx.eventBus().consumer(DHT.toAddress(prefix, myHash));
    nodeConsumer.toObservable().
      subscribe(
        msg -> processManagementMessage(msg),
        processException,
        processCompleted);
  }

  public KEY getIdentity() {
    return myHash;
  }

  public KEY getNextIdentity() {
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

  public <NODE extends DhtNode<KEY, VALUE>, RESULT extends Serializable> Observable<RESULT> traverse(KEY start, KEY end,
    RESULT identity,
    SerializableFunc2<RESULT> resultReducer,
    AsyncFunction<DhtLambda<NODE, RESULT>, RESULT> f) {
    final KEY startHash = myHash;

    byte[] ser = DHT.<NODE, RESULT>managementMessage((lambda, cb) -> {
      final NODE node = lambda.node();
      final Message<byte[]> msg = lambda.msg();

      final PublishSubject<RESULT> result = PublishSubject.create();
      result.reduce(identity, resultReducer).
        subscribe(
          r -> msg.reply(r),
          e -> msg.reply(e),
          () -> {
          });

      AtomicLong requestCounter = new AtomicLong(2);
      Runnable requestProcessed = () -> {
        if (requestCounter.decrementAndGet() == 0) {
          result.onCompleted();
        }
      };

      if ((!start.equals(end) && DHT.isResponsible(start, end, node.myHash))
        || DHT.isResponsible(node.myHash, node.nextHash, start)
        || DHT.isResponsible(node.myHash, node.nextHash, end)) {

        f.apply(new DhtLambda<>(f).node(node).msg(msg), (RESULT r) -> {
          result.onNext(r);

          requestProcessed.run();
        });
      } else {
        requestProcessed.run();
      }

      if (!node.nextHash.equals(startHash)) {
        String addr = DHT.toAddress(node.prefix, node.nextHash);
        node.vertx.eventBus().<RESULT>send(addr, lambda.serialize(), ar -> {
          if (ar.succeeded()) {
            result.onNext(ar.result().body());
          } else {
            result.onError(ar.cause());
          }

          requestProcessed.run();
        });
      } else {
        requestProcessed.run();
      }
    });
    
    return vertx.eventBus().<RESULT>sendObservable(DHT.toAddress(prefix, myHash), ser, deliveryOptions).
      map(msg -> msg.body());
  }

  @Override
  public String toString() {
    return this.getIdentity().toString() + ": ["
      + this.getIdentity().toString() + "-"
      + this.getNextIdentity().toString() + "]";
  }
}
