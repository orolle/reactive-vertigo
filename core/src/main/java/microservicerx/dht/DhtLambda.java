package microservicerx.dht;

import io.vertx.rxjava.core.eventbus.Message;
import java.io.Serializable;
import microservicerx.function.AsyncFunction;
import microservicerx.function.Serializer;
import rx.Completable;
import rx.subjects.PublishSubject;

public class DhtLambda<NODE, RESULT extends Serializable> implements Serializable {

  private static final long serialVersionUID = -2856282687873376802L;
  private static final byte[] EMPTY = Serializer.serializeAsyncFunction((p, cb) -> {
  });

  private final byte[] ser;

  private transient NODE node;
  private transient Message<byte[]> msg;
  private transient AsyncFunction<DhtLambda<NODE, RESULT>, RESULT> function;

  public DhtLambda(AsyncFunction<DhtLambda<NODE, RESULT>, RESULT> f) {
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
    if (function == null) {
      function = Serializer.deserialize(ser);
    }
  }

  public DhtLambda<NODE, RESULT> node(NODE context) {
    this.node = context;
    return this;
  }

  public DhtLambda<NODE, RESULT> msg(Message<byte[]> msg) {
    this.msg = msg;
    return this;
  }

  public <T extends NODE> T node() {
    return (T) node;
  }

  public Message<byte[]> msg() {
    return msg;
  }

  public byte[] serialize() {
    return ser;
  }

  public Completable execute() {
    init();
    PublishSubject<RESULT> result = PublishSubject.create();
    
    function.apply(this, r -> {
      result.onCompleted();
    });
    
    return result.toCompletable();
  }
}
