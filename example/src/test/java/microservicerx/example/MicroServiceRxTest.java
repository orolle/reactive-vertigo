/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.example;

import microservicerx.example.MicroServiceRx;
import microservicerx.example.MicroServiceRxVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import rvertigo.rx.DistributedObservable;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@RunWith(VertxUnitRunner.class)
public class MicroServiceRxTest {

  Vertx vertx;

  public MicroServiceRxTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testMicroServiceRxColdObservable(TestContext context) {
    Async async = context.async();

    vertx.deployVerticleObservable(MicroServiceRxVerticle.class.getCanonicalName()).
      doOnNext(id -> {
        MicroServiceRx proxy = new MicroServiceRxVertxEBProxy((io.vertx.core.Vertx) vertx.getDelegate(), MicroServiceRx.ADDRESS_DEFAULT);
        microservicerx.example.rxjava.MicroServiceRx proxy_rx = microservicerx.example.rxjava.MicroServiceRx.newInstance(proxy);

        proxy_rx.coldObservable(new JsonObject().put("name", "Hello rx-fied micro service")).
          map(json -> new DistributedObservable(json)).
          flatMap(dist -> dist.<JsonObject>toObservable(vertx)).
          doOnNext(System.out::println).
          doOnNext(json -> context.assertTrue(json.getBoolean("approved", false))).
          doOnCompleted(async::complete).
          doOnError(context::fail).
          subscribe();

      }).
      subscribe();
  }

  @Test
  public void testMicroServiceRxColdObservableFailure(TestContext context) {
    Async async = context.async();

    vertx.deployVerticleObservable(MicroServiceRxVerticle.class.getCanonicalName()).
      doOnNext(id -> {
        MicroServiceRx proxy = new MicroServiceRxVertxEBProxy((io.vertx.core.Vertx) vertx.getDelegate(), MicroServiceRx.ADDRESS_DEFAULT);
        microservicerx.example.rxjava.MicroServiceRx proxy_rx = microservicerx.example.rxjava.MicroServiceRx.newInstance(proxy);

        proxy_rx.coldObservable(new JsonObject().put("name", "bad")).
          map(json -> new DistributedObservable(json)).
          flatMap(dist -> dist.<JsonObject>toObservable(vertx)).
          doOnNext(next -> context.fail("doOnNext() should not be called!")).
          doOnError(e -> context.assertTrue(e instanceof Throwable)).
          doOnError(e -> async.complete()).
          subscribe();

      }).
      subscribe();
  }

  @Test
  public void testMicroServiceRxHotObservable(TestContext context) {
    Async async = context.async();

    vertx.deployVerticleObservable(MicroServiceRxVerticle.class.getCanonicalName()).
      doOnNext(id -> {
        MicroServiceRx proxy = new MicroServiceRxVertxEBProxy((io.vertx.core.Vertx) vertx.getDelegate(), MicroServiceRx.ADDRESS_DEFAULT);
        microservicerx.example.rxjava.MicroServiceRx proxy_rx = microservicerx.example.rxjava.MicroServiceRx.newInstance(proxy);

        proxy_rx.hotObservable(new JsonObject().put("name", "Hello rx-fied micro service")).
          map(json -> new DistributedObservable(json)).
          flatMap(dist -> dist.<JsonObject>toObservable(vertx)).
          doOnNext(System.out::println).
          doOnNext(json -> context.assertTrue(json.getBoolean("approved", false))).
          doOnCompleted(async::complete).
          doOnError(context::fail).
          subscribe();

      }).
      subscribe();
  }

  @Test
  public void testMicroServiceRxHotObservableFailure(TestContext context) {
    Async async = context.async();

    vertx.deployVerticleObservable(MicroServiceRxVerticle.class.getCanonicalName()).
      doOnNext(id -> {
        MicroServiceRx proxy = new MicroServiceRxVertxEBProxy((io.vertx.core.Vertx) vertx.getDelegate(), MicroServiceRx.ADDRESS_DEFAULT);
        microservicerx.example.rxjava.MicroServiceRx proxy_rx = microservicerx.example.rxjava.MicroServiceRx.newInstance(proxy);

        proxy_rx.hotObservable(new JsonObject().put("name", "bad")).
          map(json -> new DistributedObservable(json)).
          flatMap(dist -> dist.<JsonObject>toObservable(vertx)).
          doOnNext(next -> context.fail("doOnNext() should not be called!")).
          doOnError(e -> context.assertTrue(e instanceof Throwable)).
          doOnError(e -> async.complete()).
          subscribe();

      }).
      subscribe();
  }
}
