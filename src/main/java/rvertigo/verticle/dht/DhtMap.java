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

    this.<DhtMap<T>, Boolean>traverse(key, key, true, (lambda, v2) -> {
      lambda.node().getValues().put(key, value);
      v2.accept(true);
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

    this.<DhtMap<T>, T>traverse(key, key, null, (pair, cb) -> {
      T data = pair.node().getValues().get(key);
      cb.accept(data);
    }, (ar) -> {
      if(ar.succeeded()) {
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

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
      JsonObject o = msg.body();
      result.onNext(new AbstractMap.SimpleEntry<>((Integer) o.getValue("k"), (T) o.getValue("v")));
    });

    this.<DhtMap<T>, Boolean>traverse(from, to, Boolean.TRUE, (pair, cb) -> {
      DhtMap<T> node = pair.node();
      Observable.from(node.getValues().entrySet()).
        filter(e -> DHT.isResponsible(from, to, e.getKey())).
        subscribe(entry -> 
          node.getVertx().eventBus().publish(address,
            new JsonObject().
            put("k", entry.getKey()).
            put("v", entry.getValue())
          ),
          e -> {},
          () -> {
            System.out.println("node ["+Integer.toHexString(node.myHash)+"]");
          }
        );
    }, reply -> {
      System.out.println("RECV REPLY");
      consumer.unregister();
      result.onCompleted();
    });

    return replay;
  }
}
