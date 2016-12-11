/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.example;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@ProxyGen // Generate the proxy and handler
@VertxGen // Generate clients in non-java languages
public interface MicroServiceRx {

  String ADDRESS_DEFAULT = "micro.service.rx.example.MicroServiceRx";
  
  int NO_NAME_ERROR = 2;
  int BAD_NAME_ERROR = 3;

  void cold(JsonObject document, Handler<AsyncResult<JsonObject>> resultHandler);
  
  void hot(JsonObject document, Handler<AsyncResult<JsonObject>> resultHandler);
}
