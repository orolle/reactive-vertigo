/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.main;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import microservicerx.dht.DhtMap;
import rx.Completable;
import rx.subjects.PublishSubject;

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
    rv1 = new DhtMap<>(vertx, "DhtMap_int_int", 0);
    rv2 = new DhtMap<>(vertx, "DhtMap_int_int", 1000);
    rv3 = new DhtMap<>(vertx, "DhtMap_int_int", 2000);
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  private Completable bootstrapRunner(TestContext context) {
    Function<DhtMap, String> asString = rv -> rv.myself().myself() + " -> " + rv.myself().next();
    PublishSubject result = PublishSubject.create();

    rv1.join().
      last().
      doOnNext(node -> context.assertEquals("0 -> 0", asString.apply(rv1))).
      switchMap(v -> rv2.join()).
      last().
      doOnNext(node -> context.assertEquals("0 -> 1000", asString.apply(rv1))).
      doOnNext(node -> context.assertEquals("1000 -> 0", asString.apply(rv2))).
      switchMap(v -> rv3.join()).
      last().
      doOnNext(node -> context.assertEquals("0 -> 1000", asString.apply(rv1))).
      doOnNext(node -> context.assertEquals("1000 -> 2000", asString.apply(rv2))).
      doOnNext(node -> context.assertEquals("2000 -> 0", asString.apply(rv3))).
      last().
      doOnCompleted(() -> result.onCompleted()).
      subscribe();

    return result.toCompletable();
  }

  @Test
  public void testBootstrap(TestContext context) {
    Async async = context.async();

    bootstrapRunner(context).
      doOnError(context::fail).
      toSingleDefault("").
      toObservable().
      switchMap(v -> rv1.dhtTopologyAsDot()).
      //doOnNext(v -> System.out.println(v)).
      doOnNext(str -> context.assertEquals(5, str.split("\n").length)).
      doOnCompleted(() -> async.complete()).
      subscribe();
  }

  @Test
  public void putAndGetTest(TestContext context) {
    Async a = context.async();

    Integer testKey = 1001;
    Integer testInt = 1337;
    Set<Integer> expected = new HashSet<>();
    expected.add(testInt);

    bootstrapRunner(context).
      subscribe(() -> {
        rv2.put(testKey, testInt).
          doOnNext(context::assertTrue).
          flatMap(v -> rv3.get(testKey)).
          subscribe(value -> {
            context.assertEquals(testInt, value);
            context.assertNotEquals(System.identityHashCode(testInt), System.identityHashCode(value));
            context.assertTrue(expected.remove(value));
            context.assertTrue(expected.isEmpty());
            rv2.put(testKey, null).
              doOnError(context::fail).
              doOnCompleted(a::complete).
              subscribe();
          }, e -> {
            context.assertTrue(false, "Exception should not be thrown!");
          });
      });
  }

  //@Ignore
  @Test
  public void rangeQueryTest(TestContext context) {
    Async a = context.async();

    Arrays.asList(rv1, rv2, rv3).forEach(rv -> rv.getValues().clear());

    Integer x = 1;
    Integer y = 1002;
    Integer z = 2002;

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
            System.out.println("# e.printStackTrace()");
            e.printStackTrace();
            context.fail(e);
          }).
          doOnCompleted(() -> {
            context.assertTrue(expected.isEmpty(), "Is not empty as expected!");
            a.complete();
          }).
          subscribe();
      });
  }
}
