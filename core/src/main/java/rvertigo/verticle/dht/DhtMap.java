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
import java.util.concurrent.atomic.AtomicReference;
import rx.Observable;
import rx.subjects.BehaviorSubject;

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

  public Observable<Boolean> put(K key, T value) {
    return this.<DhtMap<K, T>, Boolean>traverse(key, null, Boolean.TRUE,
      (a, b) -> a && b,
      (lambda, cb) -> {
        lambda.node().getValues().put(key, value);
        cb.accept(Boolean.TRUE);
      });
  }

  public Observable<T> get(K key) {
    return this.<DhtMap<K, T>, T>traverse(key, null, null,
      (a, b) -> a != null ? a : b != null ? b : null,
      (pair, cb) -> {
        T data = pair.node().getValues().get(key);
        cb.accept(data);
      });
  }

  public Observable<Map.Entry<K, T>> rangeQuery(K from, K to) {
    final String address = this.prefix + ".data." + UUID.randomUUID().toString();

    BehaviorSubject<Map.Entry<K, T>> result = BehaviorSubject.create();
    AtomicLong countResponsed = new AtomicLong(0);
    AtomicReference<Runnable> checkComplete = new AtomicReference<>();

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((K) o.getValue("k"), (T) o.getValue("v")));
      countResponsed.decrementAndGet();
      checkComplete.get().run();
    });

    checkComplete.set(() -> {
      if (countResponsed.get() == 0) {
        consumer.unregister();
        result.onCompleted();
      }
    });

    this.<DhtMap<K, T>, Long>traverse(from, to, 0l,
      (a, b) -> Long.sum(a, b),
      (pair, cb) -> {
        DhtMap<K, T> node = pair.node();
        Observable.from(node.getValues().entrySet()).
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

    return result;
  }
}
