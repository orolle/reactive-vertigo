/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package micro.service.rx.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.serviceproxy.ProxyHelper;
import micro.service.rx.example.impl.MicroServiceRxImpl;

/**
 * The verticle publishing the service.
 */
public class MicroServiceRxVerticle extends AbstractVerticle {

  MicroServiceRx service;

  @Override
  public void start() throws Exception {
    // Create the client object
    service = new MicroServiceRxImpl(vertx);
    
    // Register the handler
    ProxyHelper.registerService(MicroServiceRx.class, vertx, service, MicroServiceRx.ADDRESS_DEFAULT);
  }

}