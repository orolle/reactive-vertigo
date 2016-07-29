package rvertigo.verticle.dht;

import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;
import rvertigo.function.Serializer;

public class DhtLambda<T extends Serializable, R extends Serializable> implements Processor<Void, R>, Serializable {

  private static final long serialVersionUID = -2856282687873376802L;
  private static final byte[] EMPTY = Serializer.serializeAsyncFunction((p, cb) -> {});

  private final byte[] ser;

  private transient RConsumer<R> handleResult;
  private transient List<Subscriber<? super R>> subscribers;

  private transient DhtNode<T> node;
  private transient Message<byte[]> msg;
  private transient AsyncFunction<DhtLambda<T, R>, R> function;

  public DhtLambda(AsyncFunction<DhtLambda<T, R>, R> f) {
    this(Serializer.serialize(f));
  }

  public DhtLambda() {
    this(Serializer.EMPTY);
  }

  public DhtLambda(byte[] ser) {
    this.ser = ser;
    init();
  }

  private void init() {
    if (function == null && ser != EMPTY) {
      function = Serializer.deserialize(ser);
      handleResult = (R r) -> handleResult(r);
      subscribers = new ArrayList<>();
    }
  }

  public DhtLambda<T, R> contextNode(DhtNode<T> context) {
    this.node = context;
    return this;
  }

  public DhtLambda<T, R> contextMsg(Message<byte[]> msg) {
    this.msg = msg;
    return this;
  }

  public DhtNode<T> node() {
    return node;
  }

  public Message<byte[]> msg() {
    return msg;
  }

  public byte[] serialize() {
    return ser;
  }

  private void handleResult(R r) {
    init();

    subscribers.forEach(s -> s.onNext(r));
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Void t) {
    init();

    function.apply(this, handleResult);
  }

  @Override
  public void onError(Throwable t) {
    init();

    subscribers.forEach(s -> s.onError(t));
  }

  @Override
  public void onComplete() {
    init();

    subscribers.forEach(s -> s.onComplete());
  }

  @Override
  public void subscribe(Subscriber<? super R> s) {
    init();

    subscribers.add(s);
  }

}
