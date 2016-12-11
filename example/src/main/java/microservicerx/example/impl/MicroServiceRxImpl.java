/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.example.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.serviceproxy.ServiceException;
import microservicerx.example.MicroServiceRx;
import rvertigo.rx.DistributedObservable;
import rx.Observable;
import rx.subjects.BehaviorSubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class MicroServiceRxImpl implements MicroServiceRx {

  private final Vertx vertx;

  public MicroServiceRxImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  public MicroServiceRxImpl(io.vertx.core.Vertx vertx) {
    this.vertx = new Vertx(vertx);
  }

  @Override
  public void cold(JsonObject document, Handler<AsyncResult<JsonObject>> resultHandler) {
    System.out.println("Processing...");
    Observable<JsonObject> observable;

    JsonObject result = document.copy();
    if (!document.containsKey("name")) {
      observable = Observable.error(new ServiceException(NO_NAME_ERROR, "No name in the document"));
    } else if (document.getString("name").isEmpty() || document.getString("name").equalsIgnoreCase("bad")) {
      observable = Observable.error(new ServiceException(BAD_NAME_ERROR, "Bad name in the document"));
    } else {
      result.put("approved", true);
      observable = Observable.just(result.copy().put("id", 0), result.copy().put("id", 1));
    }
    DistributedObservable dist = DistributedObservable.toDistributable(observable.map(j -> (Object) j), vertx);
    resultHandler.handle(Future.succeededFuture(dist.toJsonObject()));
  }

  @Override
  public void hot(JsonObject document, Handler<AsyncResult<JsonObject>> resultHandler) {
    System.out.println("Processing...");
    BehaviorSubject<Object> subject = BehaviorSubject.create();

    JsonObject result = document.copy();
    if (!document.containsKey("name")) {
      subject.onError(new ServiceException(NO_NAME_ERROR, "No name in the document"));
    } else if (document.getString("name").isEmpty() || document.getString("name").equalsIgnoreCase("bad")) {
      subject.onError(new ServiceException(BAD_NAME_ERROR, "Bad name in the document"));
    } else {
      Long timerId = vertx.setPeriodic(1000, l -> {
        JsonObject event = result.copy().put("approved", true).put("now", System.currentTimeMillis());
        subject.onNext(event);
      });
      
      vertx.setTimer(3*1000, l -> {
        vertx.cancelTimer(timerId);
        subject.onCompleted();
      });
    }
    DistributedObservable dist = DistributedObservable.toDistributable(subject, vertx);
    resultHandler.handle(Future.succeededFuture(dist.toJsonObject()));
  }
}
