/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.rx;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.eventbus.EventBus;
import rx.Observable;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@DataObject(generateConverter = true)
public class DhtObservable {
  public String dhtAddress;

  public DhtObservable() {
  }
  
  public DhtObservable(DhtObservable that) {
    
  }
  
  public DhtObservable(JsonObject that) {
    
  }
  
  public <T> Observable<T> create(EventBus bus) {
    return Observable.error(new IllegalStateException("NOT IMPLEMENTED"));
  }
}
