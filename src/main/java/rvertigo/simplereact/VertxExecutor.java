package rvertigo.simplereact;

import io.vertx.core.Vertx;
import java.util.concurrent.Executor;

public class VertxExecutor implements Executor {
  protected final Vertx vertx;

  public VertxExecutor(Vertx vertx) {
    super();
    this.vertx = vertx;
  }

  @Override
  public void execute(Runnable command) {
    vertx.runOnContext(v -> command.run()); // event loop, non-blocking
    //vertx.executeBlocking(v -> command.run(), null); // thread pool, blocking
  }
}
