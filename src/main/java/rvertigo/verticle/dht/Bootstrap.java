package rvertigo.verticle.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Bootstrap<T extends Serializable> {
  protected final DhtNode<T> node;
  protected final List<Consumer<Integer>> on = new ArrayList<>();

  public Bootstrap(DhtNode<T> node) {
    this.node = node;
  }
  
  public Bootstrap<T> onSuccess(Consumer<Integer> c) {
    on.add(c);
    return this;
  }

  
}
