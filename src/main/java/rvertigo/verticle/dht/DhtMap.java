/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.verticle.dht;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Counter;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import rx.Completable;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class DhtMap<T extends Serializable> extends DhtNode<T> {

  protected AsyncMap<Integer, T> values = new AsyncMap<>(this);

  public DhtMap(Vertx vertx, String prefix, Integer myHash) {
    super(vertx, prefix, myHash);
  }

  public AsyncMap<Integer, T> getValues() {
    return this.values;
  }

  public Completable put(Integer key, T value) {
    PublishSubject<Void> result = PublishSubject.<Void>create();

    this.<DhtMap<T>, Boolean>traverse(key, key, Boolean.TRUE,
      (a, b) -> a && b,
      (lambda, v2) -> {
        lambda.node().getValues().put(key, value);
        v2.accept(Boolean.TRUE);
      }, ar -> {
        if (ar.succeeded()) {
          result.onCompleted();
        } else {
          result.onError(ar.cause());
        }
      });

    return Completable.fromObservable(result);
  }

  public Observable<T> get(Integer key) {
    PublishSubject<T> result = PublishSubject.create();

    ReplaySubject<T> replay = ReplaySubject.create();
    result.subscribe(replay);

    this.<DhtMap<T>, T>traverse(key, key, null,
      (a, b) -> a != null ? a : b != null ? b : null,
      (pair, cb) -> {
        T data = pair.node().getValues().get(key);
        cb.accept(data);
      }, (ar) -> {
        if (ar.succeeded()) {
          result.onNext(ar.result());
          result.onCompleted();
        } else {
          result.onError(ar.cause());
        }
      });

    return replay;
  }

  public Observable<Map.Entry<Integer, T>> rangeQuery(Integer from, Integer to) {
    PublishSubject<Map.Entry<Integer, T>> result = PublishSubject.<Map.Entry<Integer, T>>create();

    ReplaySubject<Map.Entry<Integer, T>> replay = ReplaySubject.create();
    result.subscribe(replay);

    final String address = UUID.randomUUID().toString() + ".data";
    AtomicLong countResponsed = new AtomicLong(0);

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((Integer) o.getValue("k"), (T) o.getValue("v")));
      countResponsed.decrementAndGet();
      if (countResponsed.get() == 0) {
        result.onCompleted();
      }
    });

    result.
      doOnCompleted(() -> consumer.unregister()).
      subscribe();

    this.<DhtMap<T>, Long>traverse(from, to, 0l,
      (a, b) -> a + b,
      (pair, cb) -> {
        DhtMap<T> node = pair.node();
        Observable.from(node.getValues().entrySet()).
        filter(e -> DHT.isResponsible(from, to, e.getKey())).
        doOnNext(entry -> node.getVertx().eventBus().
          publish(address, new JsonObject().put("k", entry.getKey()).put("v", entry.getValue()))
        ).
        countLong().
        doOnNext(l -> {
          cb.accept(l);
        }).
        doOnError(e -> {
          e.printStackTrace();
        }).
        subscribe();
      }, reply -> {
        countResponsed.accumulateAndGet(reply.result(), (a, b) -> a + b);
        if (countResponsed.get() == 0) {
          result.onCompleted();
        }
      });

    return replay;
  }
}
