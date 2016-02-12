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

  public void bootstrap() {
    final Integer hash = node.myHash;

    byte[] ser = DHT.managementMessage((pair, cb) -> {
      DhtNode<?> context = (DhtNode<?>) pair.context().getValue0();
      Message<byte[]> msg = pair.context().getValue1();

      if (DHT.isResponsible(context, hash)) {
        msg.reply(context.nextHash);
        context.nextHash = hash;
      } else {
        context.vertx.eventBus().send(DHT.toAddress(context.prefix, context.nextHash), msg.body(),
          ar -> {
            if (ar.succeeded()) {
              msg.reply(ar.result().body());
            }
          });
      }

      cb.accept(null);
    });
    
    node.vertx.eventBus().send(DHT.toAddress(node.prefix, 0),
      ser,
      new DeliveryOptions().setSendTimeout(10000),
      (AsyncResult<Message<Integer>> ar) -> {
        on.forEach(c -> c.accept(ar.succeeded() ? ar.result().body() : hash));
      });
  }
}
