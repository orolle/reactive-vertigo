/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.main;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import rvertigo.function.RConsumer;
import rvertigo.verticle.ReactiveVertigo;
import rvertigo.verticle.dht.DHT;
import rx.Completable;
import rx.subjects.PublishSubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@RunWith(VertxUnitRunner.class)
public class RVertigoIntegrationTest {

  Vertx vertx;
  ReactiveVertigo<Integer> rv1, rv2, rv3;

  public RVertigoIntegrationTest() {
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
    rv1 = new ReactiveVertigo<>(vertx);
    rv2 = new ReactiveVertigo<>(vertx);
    rv3 = new ReactiveVertigo<>(vertx);
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void bootstrapTest(TestContext context) {
    Async a = context.async();
    bootstrapRunner(context).
      subscribe(() -> {
        rv1.get(0).toCompletable().subscribe(a::complete);
      });
  }

  private Completable bootstrapRunner(TestContext context) {
    PublishSubject result = PublishSubject.create();

    rv1.onJoined(r1 -> {
      context.assertTrue(r1 != null);
      rv2.onJoined(r2 -> {
        context.assertTrue(r2 != null);
        rv3.onJoined(r3 -> {
          context.assertTrue(r3 != null);
          // System.out.println("BOOTSTRAPED ALL");
          result.onCompleted();
        });
      });
    });

    return result.toCompletable();
  }

  @Test
  public void putAndGetTest(TestContext context) {
    Async a = context.async();

    Integer testInt = 1337;
    Set<Integer> expected = new HashSet<>();
    expected.add(testInt);

    bootstrapRunner(context).
      subscribe(() -> {
        rv2.put(Integer.MIN_VALUE / 2, testInt).
          toSingleDefault(Boolean.TRUE).
          flatMapObservable(v -> rv3.get(Integer.MIN_VALUE / 2)).
          subscribe(value -> {
            context.assertEquals(testInt, value);
            context.assertNotEquals(System.identityHashCode(testInt), System.identityHashCode(value));
            context.assertTrue(expected.remove(value));
            context.assertTrue(expected.isEmpty());
            a.complete();
          }, e -> {
            context.assertTrue(false, "Exception should not be thrown!");
          });
      });
  }

  @Test
  public void rangeQueryTest(TestContext context) {
    Async a = context.async();

    Integer x = Integer.MIN_VALUE / 2;
    Integer y = 0;
    Integer z = Integer.MAX_VALUE / 2;

    Set<Integer> expected = new HashSet<>();
    expected.add(y);
    expected.add(z);

    Integer from = y;
    Integer to = z + 1; // So that z is inculuded in the query range

    bootstrapRunner(context).
      subscribe(() -> {
        rv1.put(x, x).
          concatWith(rv2.put(y, y)).
          concatWith(rv3.put(z, z)).
          toSingleDefault(Boolean.TRUE).
          flatMapObservable(v -> rv1.rangeQuery(from, to)).
          subscribe(
            value -> context.assertTrue(expected.remove(value.getKey())),
            e -> context.assertTrue(false, "Exception while handling results: " + e),
            () -> {
              context.assertTrue(expected.isEmpty(), "Is not empty as expected!");
              a.complete();
            }
          );
      });
  }
}
