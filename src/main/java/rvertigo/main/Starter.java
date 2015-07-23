package rvertigo.main;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

import java.util.Date;
import java.util.UUID;

import rvertigo.function.RConsumer;
import rvertigo.verticle.ReactiveVertigo;
import rvertigo.verticle.dht.DhtNode;

public class Starter extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    super.start();

    // setup 3 nodes in DHT "cluster"
    // (these nodes are all on the same machine)
    System.out.println("# Start 3 node DHT \"cluster\"");
    new ReactiveVertigo(getVertx()).onJoined(r1 -> {
      System.out.println(" node 1 joined DHT");
      new ReactiveVertigo(getVertx()).onJoined(r2 -> {
        System.out.println(" node 2 joined DHT");
        new ReactiveVertigo(getVertx()).onJoined(r3 -> {
          System.out.println(" node 3 joined DHT");
          testFunctionality(r3);
        });
      });
    });
  }

  private void testFunctionality(ReactiveVertigo react) {
    // Store key-value pairs
    System.out.println("# Store 2 key-value pair in cluster");
    react.put(Integer.MIN_VALUE / 2, "Hello World! 1", result1 -> {
      System.out.println(" [" + Integer.toHexString(Integer.MIN_VALUE / 2) + ",Hello World! 1" + "]");
      react.put(Integer.MAX_VALUE / 2 + 1, "Hello World! 2", result2 -> {
        System.out.println(" [" + Integer.toHexString(Integer.MAX_VALUE / 2 + 1) + ",Hello World! 2" + "]");
        react.get(Integer.MIN_VALUE / 2, value -> {
        });
        react.get(Integer.MAX_VALUE / 2 + 1, value -> {
        });

        doRangeQuery(react, result -> {
          System.out.println(result.encodePrettily());
        });

        doStreaming(react);
      });
    });
  }

  public void doRangeQuery(ReactiveVertigo react, RConsumer<JsonObject> callback) {
    System.out.println("# Execute range query on the key-value pairs");

    final String uuid = UUID.randomUUID().toString();
    final JsonObject result = new JsonObject();

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(uuid + ".data", (Message<JsonObject> msg) -> {
      result.mergeIn(msg.body());
    });

    react.<Boolean> traverse(Integer.MIN_VALUE, Integer.MAX_VALUE, Boolean.TRUE, (pair, v2) -> {
      DhtNode<String> node = pair.getValue1();
      JsonObject o = new JsonObject();
      node.getValues().entrySet().forEach(e -> o.put(Integer.toHexString(e.getKey()) + "", e.getValue()));
      node.getVertx().eventBus().publish(uuid + ".data", o);
    }, reply -> {
      consumer.unregister();
      callback.accept(result);
    });
  }

  private void doStreaming(ReactiveVertigo react) {
    boolean isFirst[] = { true };
    vertx.setPeriodic(1000, h -> {
      if (isFirst[0]) {
        System.out.println("# Push events to streaming pipeline");
        isFirst[0] = false;
      }

      vertx.eventBus().publish("date", new Date().toString());
    });

    // Streaming pipeline is executed locally
    // the distributed version uses queues to distribute
    // data in the cluster
    react.<String>fromEventbus("date").
    map((str, cb) -> {
        cb.accept(" now it is " + str);
    }).
      queue(). // Distribute events in the cluster
    forEach(str -> {
      System.out.println(str);
    });
  }

  public static void main(String[] args) {
    Vertx.vertx().deployVerticle(Starter.class.getCanonicalName());
  }
}
