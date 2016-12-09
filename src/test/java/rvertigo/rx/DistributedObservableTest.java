/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.rx;

import io.vertx.core.eventbus.EventBus;
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
import rx.Observable;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@RunWith(VertxUnitRunner.class)
public class DistributedObservableTest {

  Vertx vertx;

  public DistributedObservableTest() {
    ColdObservableFSM cold = new ColdObservableFSM(null);
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
  public void writeAndReadObservable(TestContext context) {
    Async async = context.async();
    
    ((EventBus)vertx.eventBus().getDelegate()).registerDefaultCodec(DistributedObservable.class, new DistributedObservableCodec());

    DistributedObservable send = DistributedObservable.toDistributable(Observable.just(1, 2, 3), vertx);
    
    vertx.eventBus().<DistributedObservable>consumer("TEST").toObservable().
      doOnNext(msg -> {
        DistributedObservable recv = msg.body();
        
        context.assertTrue(recv != send);
        context.assertEquals(recv, send);
        
        recv.<Integer>toObservable(vertx).
          reduce(0, (r, a) -> r + a).
          doOnNext(r -> context.assertEquals(6, r)).
          doOnCompleted(async::complete).
          doOnError(context::fail).
          subscribe();
      }).
      subscribe();
    
    vertx.eventBus().send("TEST", send);
  }
}
