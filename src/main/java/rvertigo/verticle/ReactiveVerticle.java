package rvertigo.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

public class ReactiveVerticle extends AbstractVerticle {
  protected ReactiveVertigo rvertigo;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    this.rvertigo = new ReactiveVertigo(getVertx()).onJoined(node -> {
      startFuture.complete(null);
    });
  }
}
