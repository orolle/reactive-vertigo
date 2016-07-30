package rvertigo.verticle.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;

public class DhtNode<T extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final Integer myHash;
  protected Integer nextHash;

  public DhtNode(Vertx vertx, String prefix, Integer myHash) {
    this.vertx = vertx;
    this.prefix = prefix;
    this.myHash = myHash;
    this.nextHash = myHash;
  }

  public DhtNode<T> join(RConsumer<DhtNode<T>> joined) {
    new Bootstrap<>(this).
      onSuccess(nextHash -> this.nextHash = nextHash).
      onSuccess(nextHash -> onBootstraped()).
      onSuccess(nextHash -> joined.accept(this)).
      bootstrap();

    return this;
  }

  protected void onBootstraped() {
    // System.out.println("register node " + Integer.toHexString(myHash) + " to " + DHT.toAddress(prefix, 0));
    // System.out.println("register node " + Integer.toHexString(myHash)  + " to " + DHT.toAddress(prefix, myHash));
    vertx.eventBus().consumer(DHT.toAddress(prefix, 0), (Message<byte[]> msg) -> processManagementMessage(msg));
    vertx.eventBus().consumer(DHT.toAddress(prefix, myHash), (Message<byte[]> msg) -> processManagementMessage(msg));
  }

  public Integer getIdentity() {
    return myHash;
  }

  public Integer getNext() {
    return this.nextHash;
  }

  public Vertx getVertx() {
    return this.vertx;
  }

  protected void processManagementMessage(Message<byte[]> msg) {
    DhtLambda<DhtNode<T>, ? extends Serializable> l = new DhtLambda<>(msg.body());
    l.node(this);
    l.msg(msg);
    
    l.execute().
      subscribe();
  }

  public <NODE extends DhtNode<? extends Serializable>, R extends Serializable> void traverse(Integer start, Integer end, 
    R identity,
    AsyncFunction<DhtLambda<NODE, R>, R> f,
    RConsumer<AsyncResult<R>> handler) {
    final Integer hash = myHash;

    byte[] ser = DHT.<NODE, R>managementMessage((lambda, cb) -> {
      final NODE node = lambda.node();
      final Message<byte[]> msg = lambda.msg();

      if ((!start.equals(end) && DHT.isResponsible(start, end, node.myHash))
        || DHT.isResponsible(node, start) || DHT.isResponsible(node, end)) {
        f.apply(new DhtLambda<>(f).node(node).msg(msg), (R result) -> {
          msg.reply(result);
        });
      }

      if (!hash.equals(node.myHash)) {
        String addr = DHT.toAddress(node.prefix, node.nextHash);
        node.vertx.eventBus().send(addr, lambda.serialize(), ar -> {
          if (ar.succeeded()) {
            msg.reply(ar.result().body());
          } else {
            msg.reply(ar.cause());
          }
        });
      } else {
        msg.reply(identity);
      }

    });

    // System.out.println("send node " + Integer.toHexString(myHash)  + " to " + DHT.toAddress(prefix, nextHash));
    vertx.eventBus().send(DHT.toAddress(prefix, nextHash), ser, (AsyncResult<Message<R>> ar) -> {
      if (ar.succeeded()) {
        handler.accept(Future.succeededFuture(ar.result().body()));
      } else {
        handler.accept(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public String toString() {
    return Integer.toHexString(this.getIdentity()) + ": ["
      + Integer.toHexString(this.getIdentity()) + "-"
      + Integer.toHexString(this.getNext()) + "]";
  }
}
