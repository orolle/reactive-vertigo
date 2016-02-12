package rvertigo.simplereact;

import com.aol.simple.react.async.Queue;
import com.aol.simple.react.collectors.lazy.LazyResultConsumer;
import com.aol.simple.react.stream.BaseSimpleReact;
import com.aol.simple.react.stream.CloseableIterator;
import com.aol.simple.react.stream.StreamWrapper;
import com.aol.simple.react.stream.lazy.ParallelReductionConfig;
import com.aol.simple.react.stream.traits.LazyFutureStream;
import com.aol.simple.react.stream.traits.SimpleReactStream;
import com.nurkiewicz.asyncretry.RetryExecutor;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.jooq.lambda.Seq;
import org.reactivestreams.Subscriber;
import rvertigo.function.Serializer;
import io.vertx.core.eventbus.Message;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReactPipeline<U> implements Serializable {
  private static final long serialVersionUID = 8083282011271176986L;
  protected String methodName;
  protected byte[][] function;
  protected byte[] builder;
  protected transient VertxReact vertxr;
  
  protected ReactPipeline<?> previous;
  
  public ReactPipeline() {
    
  }
  
  public ReactPipeline(ReactPipeline<?> prev) {
    this();
    this.previous = prev;
  }
  
  public VertxReact vertx() {
    return this.vertxr;
  }
  
  public VertxReact vertx(VertxReact v) {
    this.vertxr = v;
    return vertx();
  }
  
  public <R> LazyFutureStream<R> unpack(LazyFutureStream<U> stream) {
    Function<LazyFutureStream<U>, LazyFutureStream<R>> c = Serializer.deserialize(builder);
    return c.apply(stream);
  }
  
  public <R> LazyFutureStream<R> unpack(VertxReact v) {
    Function<VertxReact, LazyFutureStream<R>> c = Serializer.deserialize(builder);
    return c.apply(v);
  }
  
  public <R> LazyFutureStream<R> build (VertxReact r){
    this.vertx(r);
    
    if(previous == null)
      return unpack(r);
    else
      return unpack(previous.build(r));
  }
  
  public void run (VertxReact r){
    r.vertx.executeBlocking((v) -> build(r).run(new VertxExecutor(this.vertx().vertx)), null);
  }

  public ReactPipeline<U> fromEventbus(final String addr) {
    this.builder = Serializer.serialize((Serializable & Function<VertxReact, LazyFutureStream<U>>)(VertxReact vr) -> {
      final Queue<U> queue = new Queue<>();
      
      vr.vertx.eventBus().<U>consumer(addr).handler((Message<U> msg) -> {
        queue.add(msg.body());
      });
      
      LazyFutureStream<U> stream = vr.react.
        fromStream(queue.streamCompletableFutures()).
        withParallelReduction(new ParallelReductionConfig(0, false));
      stream = stream.
        withLazyCollector(new ImmediateCollector<>(stream));
        //withLazyCollector(new BatchingCollector<>(new MaxActive(0,0), stream));
      
      return stream;
    });
    
    return new ReactPipeline<>(this);
  }
  
  protected static String getMethodName() {
    return getMethodName(3);
  }
  
  protected static String getMethodName(final int depth) {
    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    return ste[depth].getMethodName(); //Thank you Tom Tresansky
  }
  
  protected static byte[][] serializedParameter(Object... objs) {
    return Arrays.asList(objs).stream().
      map(o -> Serializer.serialize(o)).
      collect(Collectors.toList()).
      toArray(new byte[objs.length][]);
  }
  
  public static <U, R> LazyFutureStream<R> callMethod(String methodName, byte[][] functions, LazyFutureStream<U> lazyStream) {
    Method method = Arrays.asList(LazyFutureStream.class.getDeclaredMethods()).stream().
      filter(m -> methodName.equals(m.getName())).
      collect(Collectors.toList()).
      get(0);

    Object[] params = Arrays.asList(functions).stream().
      map(in -> Serializer.deserialize(in)).
      collect(Collectors.toList()).toArray();
    
    try {
      return (LazyFutureStream<R>) method.invoke(lazyStream, params);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      Logger.getLogger(ReactPipeline.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    return null;
  }
  
  //+++++++++++++++++++++++++++++++++
  //  Simple-React  API
  //+++++++++++++++++++++++++++++++++
  
  public ReactPipeline<U> filter(Predicate<? super U> predicate) {
    this.methodName = getMethodName();
    this.function = serializedParameter(predicate);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public <R> ReactPipeline<R> map(Function<? super U, ? extends R> mapper) {
    this.methodName = getMethodName();
    this.function = serializedParameter(mapper);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<R>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public IntStream mapToInt(ToIntFunction<? super U> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public LongStream mapToLong(ToLongFunction<? super U> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public DoubleStream mapToDouble(ToDoubleFunction<? super U> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <R> ReactPipeline<R> flatMap(Function<? super U, ? extends Stream<? extends R>> mapper) {
    this.methodName = getMethodName();
    this.function = serializedParameter(mapper);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<R>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public IntStream flatMapToInt(Function<? super U, ? extends IntStream> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public LongStream flatMapToLong(Function<? super U, ? extends LongStream> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public DoubleStream flatMapToDouble(Function<? super U, ? extends DoubleStream> mapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public ReactPipeline<U> distinct() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> sorted() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> sorted(Comparator<? super U> comparator) {
    this.methodName = getMethodName();
    this.function = serializedParameter(comparator);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> peek(Consumer<? super U> action) {
    this.methodName = getMethodName();
    this.function = serializedParameter(action);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> limit(long maxSize) {
    this.methodName = getMethodName();
    this.function = serializedParameter(maxSize);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> skip(long n) {
    this.methodName = getMethodName();
    this.function = serializedParameter(n);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> onClose(Runnable closeHandler) {
    this.methodName = getMethodName();
    this.function = serializedParameter(closeHandler);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<Void> close() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<Void> forEach(Consumer<? super U> action) {
    this.methodName = getMethodName();
    this.function = serializedParameter(action);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }
  
  public ReactPipeline<Void> forEachOrdered(Consumer<? super U> action) {
    this.methodName = getMethodName();
    this.function = serializedParameter(action);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }
  
  public U reduce(U identity, BinaryOperator<U> accumulator) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Optional<U> reduce(BinaryOperator<U> accumulator) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  
	public <T> T reduce(T identity, BiFunction<T,? super U,T> accumulator, BinaryOperator<T> combiner){
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super U> accumulator, BiConsumer<R, R> combiner) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <R, A> R collect(Collector<? super U, A, R> collector) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Optional<U> min(Comparator<? super U> comparator) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Optional<U> max(Comparator<? super U> comparator) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public long count() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public boolean anyMatch(Predicate<? super U> predicate) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public boolean allMatch(Predicate<? super U> predicate) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public boolean noneMatch(Predicate<? super U> predicate) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Optional<U> findFirst() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Optional<U> findAny() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public CloseableIterator<U> iterator() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Spliterator<U> spliterator() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public boolean isParallel() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public ReactPipeline<U> sequential() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> parallel() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public ReactPipeline<U> unordered() {
    this.methodName = getMethodName();
    this.function = serializedParameter();
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<U>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public List getOriginalFutures() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withTaskExecutor(Executor e) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withRetrier(RetryExecutor retry) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withWaitStrategy(Consumer<CompletableFuture> c) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withLazyCollector(LazyResultConsumer<U> lazy) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  public SimpleReactStream<U> withLastActive(StreamWrapper streamWrapper) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withErrorHandler(Optional<Consumer<Throwable>> errorHandler) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public SimpleReactStream<U> withAsync(boolean b) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Queue<U> toQueue() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <K> void toQueue(Map<K, Queue<U>> shards, Function<U, K> sharder) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Queue<U> toQueue(Function<Queue, Queue> modifier) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <T, R> ReactPipeline<R> allOf(Collector collector, Function<T, R> fn) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <R> ReactPipeline<R> then(Function<U, R> fn, Executor exec) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <R> ReactPipeline<R> thenSync(Function<U, R> fn) {
    this.methodName = getMethodName();
    this.function = serializedParameter(fn);
    this.builder = Serializer.serialize((Serializable & Function<LazyFutureStream<U>, LazyFutureStream<R>>)( LazyFutureStream<U> s) -> {
      return callMethod(methodName, function, s);
    });
    
    return new ReactPipeline<>(this);
  }

  
  public <T extends BaseSimpleReact> T getPopulator() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public <T extends BaseSimpleReact> void returnPopulator(T service) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public void cancel() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public void forwardErrors(Consumer<Throwable> c) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public Executor getPublisherExecutor() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  
  public void subscribe(Subscriber<? super U> s) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
	
  
}
