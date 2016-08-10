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
public class DhtMap<K extends Serializable & Comparable<K>, T extends Serializable> extends DhtNode<K, T> {

  protected AsyncMap<K, T> values = new AsyncMap<>(this);

  public DhtMap(Vertx vertx, String prefix, K myHash) {
    super(vertx, prefix, myHash);
  }

  public AsyncMap<K, T> getValues() {
    return this.values;
  }

  public Completable put(K key, T value) {
    PublishSubject<Void> result = PublishSubject.<Void>create();

    this.<DhtMap<K, T>, Boolean>traverse(key, key, Boolean.TRUE,
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

  public Observable<T> get(K key) {
    ReplaySubject<T> result = ReplaySubject.create();
    
    this.<DhtMap<K, T>, T>traverse(key, key, null,
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

    return result;
  }

  public Observable<Map.Entry<K, T>> rangeQuery(K from, K to) {
    ReplaySubject<Map.Entry<K, T>> result = ReplaySubject.create();

    final String address = this.prefix + ".data." + UUID.randomUUID().toString();
    AtomicLong countResponsed = new AtomicLong(0);

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((K) o.getValue("k"), (T) o.getValue("v")));
      countResponsed.decrementAndGet();
      if (countResponsed.get() == 0) {
        result.onCompleted();
      }
    });

    result.
      doOnCompleted(() -> consumer.unregister()).
      subscribe();

    this.<DhtMap<K, T>, Long>traverse(from, to, 0l,
      (a, b) -> {
        return a + b;
      },
      (pair, cb) -> {
        DhtMap<K, T> node = pair.node();
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
        long count = countResponsed.accumulateAndGet(reply.result(), (a, b) -> a + b);
        if (countResponsed.get() == 0) {
          result.onCompleted();
        }
      });

    return result;
  }
}
