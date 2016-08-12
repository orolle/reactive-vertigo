/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.main;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import rvertigo.verticle.dht.DhtMap;
import rx.Completable;
import rx.subjects.PublishSubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@RunWith(VertxUnitRunner.class)
public class DhtMapStringStringTest {
  
  Vertx vertx;
  DhtMap<String, String> map1, map2, map3;

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
    map1 = new DhtMap<>(vertx, "dht.map_string_string", "A");
    map2 = new DhtMap<>(vertx, "dht.map_string_string", "Z");
    map3 = new DhtMap<>(vertx, "dht.map_string_string", "a");
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
        map1.get("").toCompletable().subscribe(a::complete);
      });
  }

  private Completable bootstrapRunner(TestContext context) {
    PublishSubject result = PublishSubject.create();

    map1.join(r1 -> {
      context.assertTrue(r1 != null);
      map2.join(r2 -> {
        context.assertTrue(r2 != null);
        map3.join(r3 -> {
          context.assertTrue(r3 != null);
          result.onCompleted();
        });
      });
    });

    return result.toCompletable();
  }

  @Test
  public void putAndGetTest(TestContext context) {
    Async a = context.async();

    String testKey = "HELLO WORLD!";
    String testValue = "WORKS";
    Set<String> expected = new HashSet<>();
    expected.add(testValue);

    bootstrapRunner(context).
      subscribe(() -> {
        map2.put(testKey, testValue).
          toSingleDefault(Boolean.TRUE).
          flatMapObservable(v -> map3.get(testKey)).
          subscribe(value -> {
            context.assertEquals(testValue, value);
            context.assertNotEquals(System.identityHashCode(testValue), System.identityHashCode(value));
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

    String x = "HELLO WORLD!";
    String y = "hello world!";
    String z = "zzzzzzz";

    Set<String> expected = new HashSet<>();
    expected.add(x);
    expected.add(y);

    String from = "A";
    String to = "i"; // So that y is inculuded in the query range
    
    context.assertTrue(from.compareTo(x) <= 0);
    context.assertTrue(from.compareTo(y) <= 0);
    context.assertTrue(to.compareTo(x) > 0);
    context.assertTrue(to.compareTo(y) > 0);
    
    bootstrapRunner(context).
      subscribe(() -> {
        map1.put(x, x).
          concatWith(map2.put(y, y)).
          concatWith(map3.put(z, z)).
          toSingleDefault(Boolean.TRUE).
          flatMapObservable(v -> map1.rangeQuery(from, to)).
          subscribe(
            value -> {
              context.assertTrue(expected.remove(value.getKey()));
            },
            e -> context.assertTrue(false, "Exception while handling results: " + e),
            () -> {
              context.assertTrue(expected.isEmpty(), "Is not empty as expected!");
              a.complete();
            }
          );
      });
  }
}
