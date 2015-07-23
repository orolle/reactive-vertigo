package rvertigo.queue;

import io.vertx.core.Vertx;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class QueueIn<T extends Serializable> implements Processor<T, Void> {
  protected Vertx vertx;
  protected final List<Subscriber<? super Void>> subscribers = new ArrayList<>();

  protected String address;

  public QueueIn(String address) {
    this.address = address;
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T t) {
    vertx.eventBus().publish(address, t);
  }

  @Override
  public void onError(Throwable t) {
    throw new IllegalStateException("NOT IMPLEMENTED YET");
  }

  @Override
  public void onComplete() {
    throw new IllegalStateException("NOT IMPLEMENTED YET");
  }

  @Override
  public void subscribe(Subscriber<? super Void> s) {
    subscribers.add(s);
  }

}
