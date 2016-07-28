package rvertigo.stream;

import io.vertx.core.Vertx;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rvertigo.function.AsyncFunction;
import rvertigo.function.RConsumer;

class ReactiveSubscription<T> implements Subscription, Serializable {
  private static final long serialVersionUID = 2312848497981354634L;

  @Override
  public void request(long n) {

  }

  @Override
  public void cancel() {

  }
}

public abstract class ReactivePipeline<In, Out> extends ReactiveStreamAdapter<Out> implements Processor<In, Out> {
  protected final List<Subscriber<? super Out>> subscribers = new ArrayList<>();
  protected final AsyncFunction<? super In, ? extends Out> function;
  protected final Vertx vertx;

  public ReactivePipeline(Vertx vertx, AsyncFunction<? super In, ? extends Out> function) {
    this.function = function;
    this.vertx = vertx;
  }

  @Override
  public void onNext(In t) {
    function.apply(t, o -> subscribers.forEach(s -> s.onNext(o)));
  }

  @Override
  public void onError(Throwable t) {
    throw new IllegalStateException(t);
  }

  @Override
  public void onComplete() {

  }

  @Override
  public void subscribe(Subscriber<? super Out> s) {
    subscribers.add(s);
    s.onSubscribe(new ReactiveSubscription<>());
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public <R> ReactiveStream<R> map(AsyncFunction<? super Out, ? extends R> mapper) {
    ReactivePipeline<Out, R> next = new ReactivePipeline.StatelessOp<Out, R>(vertx, mapper);
    this.subscribe(next);
    return next;
  }

  @Override
  public void forEach(Consumer<? super Out> action) {
    ReactivePipeline<Out, Void> next = new ReactivePipeline.Sink<Out>(vertx, data -> action.accept(data));
    this.subscribe(next);
  }

  @Override
  public ReactiveStream<Out> queue() {
    String uuid = UUID.randomUUID().toString();

    ReactivePipeline<Out, Void> queueIn = new ReactivePipeline.Sink<Out>(vertx, data -> {
      vertx.eventBus().publish(uuid, data);
    });
    this.subscribe(queueIn);

    ReactivePipeline.Source<Out> queueOut = new ReactivePipeline.Source<Out>(vertx);
    vertx.eventBus().<Out> consumer(uuid).handler(msg -> {
      queueOut.callback().accept(msg.body());
    });
    return queueOut;
  }

  public static class Source<Out> extends ReactivePipeline<Void, Out> {
    protected final RConsumer<Out> callback = (data) -> {
      subscribers.forEach(s -> s.onNext(data));
    };

    public Source(Vertx vertx) {
      super(vertx, null);
    }

    public RConsumer<Out> callback() {
      return callback;
    }
  }

  public static class Sink<In> extends ReactivePipeline<In, Void> {
    public Sink(Vertx vertx, RConsumer<In> c) {
      super(vertx, (data, cb) -> {
        c.accept(data);
      });
    }
  }

  public static class StatelessOp<In, Out> extends ReactivePipeline<In, Out> {
    public StatelessOp(Vertx vertx, AsyncFunction<? super In, ? extends Out> mapper) {
      super(vertx, mapper);
    }
  }

  public static class StatefulOp<In, Out> extends ReactivePipeline<In, Out> {
    public StatefulOp(Vertx vertx, AsyncFunction<? super In, ? extends Out> mapper) {
      super(vertx, mapper);
    }
  }
}
