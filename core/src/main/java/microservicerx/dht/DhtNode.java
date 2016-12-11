package microservicerx.dht;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import microservicerx.function.AsyncFunction;
import microservicerx.function.SerializableFunc2;
import microservicerx.dht.routing.NodeInformation;
import microservicerx.dht.routing.Routing;
import microservicerx.dht.routing.SerializableCodec;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

public class DhtNode<KEY extends Serializable & Comparable<KEY>, VALUE extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final Routing<KEY> routing;

  protected final DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(10000);

  protected MessageConsumer<byte[]> broadcastConsumer;
  protected MessageConsumer<byte[]> nodeConsumer;

  public DhtNode(Vertx vertx, String prefix, KEY myHash) {
    this.vertx = vertx;
    this.prefix = prefix;

    setupDependencies();

    this.routing = new Routing<>();
    this.routing.myself().
      myself(myHash).
      next(myHash).
      previous(myHash);
  }

  public NodeInformation<KEY> myself() {
    return routing.myself();
  }

  public Observable<DhtNode<KEY, VALUE>> join() {
    return bootstrap().
      doOnNext(nodeInfo -> this.myself().next(nodeInfo.next())).
      doOnCompleted(() -> onBootstraped()).
      last().
      map(s -> this);
  }

  public Observable<NodeInformation<KEY>> bootstrap() {
    final KEY hash = this.myself().myself();

    byte[] ser = DHT.managementMessage((lambda, cb) -> {
      DhtNode<KEY, VALUE> node = lambda.node();
      Message<byte[]> msg = lambda.msg();

      if (DHT.isResponsible(node.myself().myself(), node.myself().next(), hash)) {
        msg.reply(node.myself());
        node.myself().next(hash);

      } else {
        node.vertx.eventBus().
          <KEY>sendObservable(DHT.toAddress(node.prefix, node.myself().next()), msg.body(), node.deliveryOptions).
          subscribe(reply -> msg.reply(reply.body()));
      }

      cb.accept(null);
    });

    return this.vertx.eventBus().
      <NodeInformation<KEY>>sendObservable(this.prefix, ser, deliveryOptions).
      map(msg -> msg.body()).
      onErrorResumeNext(e -> Observable.just(this.myself()));
  }

  protected void onBootstraped() {
    Action1<Throwable> processException = e -> {
      e.printStackTrace();
    };
    Action0 processCompleted = () -> {
    };

    nodeConsumer = vertx.eventBus().consumer(DHT.toAddress(prefix, myself().myself()));
    nodeConsumer.toObservable().
      subscribe(
        this::processManagementMessage,
        processException,
        processCompleted);

    broadcastConsumer = vertx.eventBus().consumer(prefix);
    broadcastConsumer.toObservable().
      subscribe(
        this::processManagementMessage,
        processException,
        processCompleted);
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

  public <NODE extends DhtNode<KEY, VALUE>, RESULT extends Serializable> Observable<RESULT> traverse(KEY start,
    KEY end,
    RESULT identity,
    SerializableFunc2<RESULT> resultReducer,
    AsyncFunction<DhtLambda<NODE, RESULT>, RESULT> f) {
    // TODO 
    // Replace traverse() logic with meaningful interfaces and state machine to be easier to understand
    // TODO

    KEY initator = myself().myself();
    
    byte[] ser = DHT.<NODE, RESULT>managementMessage((lambda, cb) -> {
      // on REMOTE node
      final NODE remote = lambda.node();
      final Message<byte[]> msg = lambda.msg();
      final AtomicReference<RequestStatus> status = new AtomicReference(RequestStatus.PROCESSING_LOCAL);

      final PublishSubject<RESULT> result = PublishSubject.create();
      result.
        reduce(identity, resultReducer).
        doOnError(e -> msg.fail(-1, e.getMessage())).
        doOnNext(r -> msg.reply(r)).
        subscribe();

      Consumer<RequestStatus> requestProcessed = (s) -> {
        if (status.get() == RequestStatus.PROCESSING_LOCAL && s == RequestStatus.FINISHED) {
          status.set(s);
        } else if (status.get() == RequestStatus.PROCESSING_NEXT && s == RequestStatus.FINISHED) {
          status.set(s);
        } else if (status.get() == RequestStatus.PROCESSING_LOCAL && s == RequestStatus.PROCESSING_NEXT) {
          status.set(s);
        } else if (status.get() == s) {
        } else {
          throw new IllegalStateException(status.get() + " -> " + s + " NOT POSSIBLE");
        }

        if (status.get() == RequestStatus.FINISHED && !result.hasCompleted()) {
          result.onNext(identity);
          result.onCompleted();
        }
      };

      if (DHT.isResponsible(remote.myself().myself(), remote.myself().next(), start)
        || DHT.isResponsible(remote.myself().myself(), remote.myself().next(), end != null? end : start)
        || (end != null && !start.equals(end) && DHT.isResponsible(start, end, remote.myself().myself()))) {

        f.apply(new DhtLambda<>(f).node(remote).msg(msg), (RESULT r) -> {
          result.onNext(r);

          if (start.equals(end)) {
            requestProcessed.accept(RequestStatus.FINISHED);
          } else {
            requestProcessed.accept(RequestStatus.PROCESSING_NEXT);
          }
        });
      } else {
        requestProcessed.accept(RequestStatus.PROCESSING_NEXT);
      }

      if (!remote.myself().next().equals(initator)
        && status.get() == RequestStatus.PROCESSING_NEXT) {
        String addr = DHT.toAddress(remote.prefix, remote.myself().next());
        remote.vertx.eventBus().<RESULT>sendObservable(addr, lambda.serialize(), remote.deliveryOptions).
          doOnNext(msg1 -> result.onNext(msg1.body())).
          doOnError(e -> result.onError(e)).
          doOnCompleted(() -> requestProcessed.accept(RequestStatus.FINISHED)).
          subscribe();

      } else {
        requestProcessed.accept(RequestStatus.FINISHED);
      }
    });

    return vertx.eventBus().<RESULT>sendObservable(DHT.toAddress(prefix, myself().myself()), ser, deliveryOptions).
      map(msg -> msg.body());
  }

  public Observable<Map.Entry<KEY, KEY>> dhtTopology() {
    final String address = this.prefix + ".data." + UUID.randomUUID().toString();
    final KEY from = myself().next();
    final KEY to = myself().myself();

    ReplaySubject<Map.Entry<KEY, KEY>> result = ReplaySubject.create();
    AtomicLong countResponsed = new AtomicLong(0);
    AtomicReference<Runnable> checkComplete = new AtomicReference<>();

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((KEY) o.getValue("k"), (KEY) o.getValue("v")));
      countResponsed.decrementAndGet();
      checkComplete.get().run();
    });

    checkComplete.set(() -> {
      if (countResponsed.get() == 0) {
        consumer.unregister();
        result.onCompleted();
      }
    });

    result.onNext(new AbstractMap.SimpleEntry<>(myself().myself(), myself().next()));

    this.<DhtMap<KEY, VALUE>, Long>traverse(from, to, 0l,
      (a, b) -> Long.sum(a, b),
      (pair, cb) -> {
        DhtMap<KEY, VALUE> node = pair.node();
        Observable.just(new AbstractMap.SimpleEntry<>(node.myself().myself(), node.myself().next())).
        filter(e -> DHT.isResponsible(from, to, e.getKey())).
        doOnNext(entry -> {
          node.getVertx().eventBus().
            publish(address, new JsonObject().put("k", entry.getKey()).put("v", entry.getValue()));
        }).
        countLong().
        subscribe(l -> {
          cb.accept(l);
        }, e -> {
          e.printStackTrace();
        });
      }).
      subscribe(l -> {
        long count = countResponsed.accumulateAndGet(l, (a, b) -> a + b);
        checkComplete.get().run();
      }, e -> {
        e.printStackTrace();
      });

    return result.
      cache();
  }

  public Observable<String> dhtTopologyAsDot() {
    return Observable.empty().concat(
      Observable.just("digraph G {"),
      dhtTopology().map(e -> "  " + e.getKey() + " -> " + e.getValue()),
      Observable.just("}")
    ).
      reduce(new ArrayList<String>(), (a, b) -> {
        a.add(b);
        return a;
      }).
      map(list -> String.join("\n", list));
  }

  @Override
  public String toString() {
    return this.myself().myself().toString() + ": ["
      + this.myself().previous().toString() + "-"
      + this.myself().next().toString() + "]";
  }

  private void setupDependencies() {
    try {
      SerializableCodec codec = new SerializableCodec(NodeInformation.class);
      ((io.vertx.core.Vertx) this.vertx.getDelegate()).eventBus().
        registerDefaultCodec(codec.klass, codec);
    } catch (IllegalStateException e) {

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  enum RequestStatus {
    PROCESSING_LOCAL, FINISHED, PROCESSING_NEXT
  }
}
