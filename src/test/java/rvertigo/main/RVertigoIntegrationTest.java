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
    bootstrapRunner(context, v -> {
      a.complete();
    });
  }

  private void bootstrapRunner(TestContext context, RConsumer<Void> completed) {
    rv1.onJoined(r1 -> {
      context.assertTrue(r1 != null);
      rv2.onJoined(r2 -> {
        context.assertTrue(r2 != null);
        rv3.onJoined(r3 -> {
          context.assertTrue(r3 != null);
          completed.accept(null);
        });
      });
    });
  }

  @Test
  public void putAndGetTest(TestContext context) {
    Async a = context.async();
    Integer testInt = 1337;

    bootstrapRunner(context, v -> {
      rv2.put(Integer.MIN_VALUE / 2, testInt, f -> {
        rv3.get(Integer.MIN_VALUE / 2, value -> {
          context.assertEquals(testInt, value);
          context.assertNotEquals(System.identityHashCode(testInt), System.identityHashCode(value));
          a.complete();
        });
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

    bootstrapRunner(context, v -> {
      rv1.put(x, x, f1 -> {
        rv2.put(y, y, f2 -> {
          rv3.put(z, z, f3 -> {
            rv1.
              rangeQuery(y, z).
              subscribe(
                value -> context.assertTrue(expected.remove(value.getKey())),
                e -> context.assertTrue(false, "Exception while handling results: " + e),
                () -> {
                  context.assertTrue(expected.isEmpty(), "Is not empty as expected!");
                  a.complete();
                }
              );
          });
        });
      });

    });
  }
}
