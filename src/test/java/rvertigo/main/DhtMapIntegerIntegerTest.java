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
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import rvertigo.verticle.dht.DhtMap;
import rx.Completable;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
@RunWith(VertxUnitRunner.class)
public class DhtMapIntegerIntegerTest {

  Vertx vertx;
  DhtMap<Integer, Integer> rv1, rv2, rv3;

  Random rand = new Random(10);

  private Integer random() {
    return rand.nextBoolean() ? rand.nextInt(Integer.MAX_VALUE) : -rand.nextInt(Integer.MAX_VALUE);
  }

  public DhtMapIntegerIntegerTest() {
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
    rv1 = new DhtMap<>(vertx, "DhtMap_int_int", random());
    rv2 = new DhtMap<>(vertx, "DhtMap_int_int", random());
    rv3 = new DhtMap<>(vertx, "DhtMap_int_int", random());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  private Completable bootstrapRunner(TestContext context) {
    PublishSubject result = PublishSubject.create();

    rv1.join(r1 -> {
      context.assertTrue(r1 != null);
      rv2.join(r2 -> {
        context.assertTrue(r2 != null);
        rv3.join(r3 -> {
          context.assertTrue(r3 != null);
          // System.out.println("BOOTSTRAPED ALL");
          result.onCompleted();
        });
      });
    });

    return result.toCompletable();
  }

  @Ignore
  @Test
  public void testBootstrap(TestContext context) {
    Async async = context.async();

    bootstrapRunner(context).
      doOnError(context::fail).
      doOnCompleted(() -> async.complete()).
      subscribe();
  }
  
  @Ignore
  @Test
  public void putAndGetTest(TestContext context) {
    Async a = context.async();

    Integer testInt = 1337;
    Set<Integer> expected = new HashSet<>();
    expected.add(testInt);

    bootstrapRunner(context).
      subscribe(() -> {
        rv2.put(Integer.MIN_VALUE / 2, testInt).
          doOnNext(b -> System.out.println("SUCCEED: " + b)).
          doOnNext(context::assertTrue).
          flatMap(v -> rv3.get(Integer.MIN_VALUE / 2)).
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

    Integer from = y - 1;
    Integer to = z + 1; // So that z is inculuded in the query range

    context.assertTrue(from < y);
    context.assertTrue(from < z);
    context.assertTrue(to > y);
    context.assertTrue(to > z);

    bootstrapRunner(context).
      subscribe(() -> {
        rv1.put(x, x).
          concatWith(rv2.put(y, y)).
          concatWith(rv3.put(z, z)).
          last().
          flatMap(v -> rv1.rangeQuery(from, to)).
          doOnNext(value -> context.assertTrue(expected.remove(value.getKey()))).
          doOnError(e -> {
            context.assertTrue(false, "Exception while handling results. ");
            e.printStackTrace();
          }).
          doOnCompleted(() -> {
            context.assertTrue(expected.isEmpty(), "Is not empty as expected!");
            a.complete();
          }).
          subscribe();
      });
  }

  @Ignore
  @Test
  public void replaySubjectTes() {
    ReplaySubject<String> replay = ReplaySubject.create();

    replay.onNext("A");
    replay.onNext("B");
    replay.onNext("C");

    replay.onCompleted();

    replay.onNext("D");

    replay.onCompleted();

    replay.
      doOnNext(System.out::println).
      subscribe();
  }
}
