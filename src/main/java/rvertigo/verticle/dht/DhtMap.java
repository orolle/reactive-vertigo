/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.verticle.dht;

import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import rx.Completable;
import rx.Observable;
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
    return this.<DhtMap<K, T>, Boolean>traverse(key, key, Boolean.TRUE,
      (a, b) -> a && b,
      (lambda, v2) -> {
        lambda.node().getValues().put(key, value);
        v2.accept(Boolean.TRUE);
      }).
      toCompletable();
  }

  public Observable<T> get(K key) {
    return this.<DhtMap<K, T>, T>traverse(key, key, null,
      (a, b) -> a != null ? a : b != null ? b : null,
      (pair, cb) -> {
        T data = pair.node().getValues().get(key);
        cb.accept(data);
      });
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
        subscribe(l -> {
          cb.accept(l);
        }, e -> {
          e.printStackTrace();
        });
      }).
      subscribe(l -> {
        long count = countResponsed.accumulateAndGet(l, (a, b) -> a + b);
        if (countResponsed.get() == 0) {
          result.onCompleted();
        }
      }, e -> {
        e.printStackTrace();
      });

    return result;
  }
}
