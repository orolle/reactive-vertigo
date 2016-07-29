package rvertigo.main;

import com.aol.simple.react.stream.lazy.LazyReact;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import rvertigo.function.RConsumer;
import rvertigo.simplereact.VertxExecutor;
import rvertigo.simplereact.VertxReact;
import rvertigo.verticle.ReactiveVertigo;
import rvertigo.verticle.dht.DhtNode;

public class Starter extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    super.start();
    System.out.println("SERIALIZED FUNCTION PoC");

    // setup 3 nodes in DHT "cluster"
    // (these nodes are all on the same machine)
    System.out.println("# Start 3 node DHT \"cluster\"");
    new ReactiveVertigo<String>(getVertx()).onJoined(r1 -> {
      System.out.println(" node 1 joined DHT");
      new ReactiveVertigo<String>(getVertx()).onJoined(r2 -> {
        System.out.println(" node 2 joined DHT");
        new ReactiveVertigo<String>(getVertx()).onJoined(r3 -> {
          System.out.println(" node 3 joined DHT");
          testFunctionality(r3);
        });
      });
    });

    LazyReact react = new LazyReact(new VertxExecutor(getVertx())).withAsync(false);
    int number = react.of(1, 2, 3).map(i -> i + 1).reduce((a, b) -> a + b).orElse(Integer.MIN_VALUE);
    System.out.println("sum = " + number); // 2 + 3 + 4 = 9
  }

  // Store key-value pairs
  private void testFunctionality(ReactiveVertigo react) {
    System.out.println("# Store 2 key-value pair in cluster");

    react.put(Integer.MIN_VALUE / 2, "Hello World! 1").
      concatWith(react.put(Integer.MAX_VALUE / 2 + 1, "Hello World! 2")).
      doOnCompleted(() -> {
        System.out.println(" [" + Integer.toHexString(Integer.MAX_VALUE / 2 + 1) + ",Hello World! 2" + "]");
        react.get(Integer.MIN_VALUE / 2).subscribe(System.out::println);
        react.get(Integer.MAX_VALUE / 2 + 1).subscribe(System.out::println);

        doRangeQuery(react, result -> {
          System.out.println(result.encodePrettily());
          System.exit(0);
        });
      });

  }

  public void doRangeQuery(ReactiveVertigo<String> react, RConsumer<JsonObject> callback) {
    System.out.println("# Execute range query on the key-value pairs");

    final String uuid = UUID.randomUUID().toString();
    final JsonObject result = new JsonObject();

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(uuid + ".data", (Message<JsonObject> msg) -> {
      result.mergeIn(msg.body());
    });

    react.<Boolean>traverse(Integer.MIN_VALUE, Integer.MAX_VALUE, Boolean.TRUE, (pair, v2) -> {
      DhtNode<String> node = pair.context();
      JsonObject o = new JsonObject();
      node.getValues().entrySet().forEach(e -> o.put(Integer.toHexString(e.getKey()) + "", e.getValue()));
      node.getVertx().eventBus().publish(uuid + ".data", o);
    }, reply -> {
      consumer.unregister();
      callback.accept(result);
    });
  }

  /*
   * Streaming out of scope
  private void doStreaming(ReactiveVertigo react_old) {
    boolean isFirst[] = {true};
    VertxReact react = new VertxReact(vertx);

    vertx.setPeriodic(1000, h -> {
      if (isFirst[0]) {
        System.out.println("# Push events to streaming pipeline");
        isFirst[0] = false;
      }

      vertx.eventBus().publish("date", new Date().toString());
    });

    // Streaming out of scope
    //
    // Streaming pipeline is executed locally
    // the distributed version uses queues to distribute
    // data in the cluster
    //vertx.executeBlocking(v -> {
    react.<String>fromEventbus("date").
      map((Serializable & Function<String, String>) (str) -> {
        return (" now it is " + str);
      }).
      forEach((Serializable & Consumer<String>) (str) -> {
        System.out.println(str);
      }).
      run(react);
    //}, null);
  }
   */
  public static void main(String[] args) {
    Vertx.vertx().deployVerticle(Starter.class.getCanonicalName());
    // react.async.Queue;

    Vertx.vertx().setTimer(30000, s -> {
      System.err.println("STOP JAVA PROCESS");
      System.err.println("  Starter.java  ");
      System.err.println("CLEANUP VERTX RESSOURCES");
      System.exit(-1);
    });
  }
}
