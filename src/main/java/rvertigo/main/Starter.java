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
    System.err.println("NOTHIN IMPLEMENTED YET!");
  }

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
