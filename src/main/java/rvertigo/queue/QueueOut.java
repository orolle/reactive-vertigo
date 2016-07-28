package rvertigo.queue;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class QueueOut<T extends Serializable> implements Processor<Void, T> {
  protected List<Subscriber<? super T>> subscribers = new ArrayList<>();
  protected Vertx vertx;

  public QueueOut(String address) {
    this.register(address);
  }

  private void register(String address) {
    vertx.eventBus().consumer(address, (Message<T> msg) -> {
      this.subscribers.forEach(s -> s.onNext(msg.body()));
    });
  }

  @Override
  public void subscribe(Subscriber<? super T> s) {
    this.subscribers.add(s);
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Void t) {

  }

  @Override
  public void onError(Throwable t) {

  }

  @Override
  public void onComplete() {

  }
}
