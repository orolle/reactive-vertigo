/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package micro.service.rx.example.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.serviceproxy.ServiceException;
import micro.service.rx.example.MicroServiceRx;
import rvertigo.rx.DistributedObservable;
import rx.Observable;

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
  public void process(JsonObject document, Handler<AsyncResult<JsonObject>> resultHandler) {
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
}
