/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.simplereact;

import com.aol.simple.react.collectors.lazy.LazyResultConsumer;
import com.aol.simple.react.config.MaxActive;
import com.aol.simple.react.stream.traits.BlockingStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.aol.simple.react.config.MaxActive;
import com.aol.simple.react.stream.MissingValue;
import com.aol.simple.react.stream.traits.BlockingStream;
import com.aol.simple.react.stream.traits.ConfigurableStream;

/**
 * This class allows a Batch of completable futures to be processed before collecting their results, to increase
 * parallelism.
 * 
 * @author johnmcclean
 *
 * @param <T> Result type
 */
public class ImmediateCollector<T> implements LazyResultConsumer<T>{

	private Collection<CompletableFuture<T>> results;
	private final List<CompletableFuture<T>> active = new ArrayList<>();
	private final BlockingStream<T> blocking;

	private final MaxActive maxActive;
	
	/**
	 * @param maxActive Controls batch size
	 */
	public ImmediateCollector(MaxActive maxActive,BlockingStream<T> blocking){
		this.results =null;
		this.blocking = blocking;
    this.maxActive = maxActive;
	}
	/**
	 * Batching Collector with default Max Active settings
	 */
	public ImmediateCollector(BlockingStream<T> blocking){
		this(new MaxActive(0, 0), blocking);
	}
	
	/* (non-Javadoc)
	 * @see java.util.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(CompletableFuture<T> t) {
		active.add(t);
		
		if(active.size()>maxActive.getMaxActive()){
			
			while(active.size()>maxActive.getReduceTo()){
				List<CompletableFuture<T>> toRemove = active.stream().filter(cf -> cf.isDone()).collect(Collectors.toList());
				active.removeAll(toRemove);
				results.addAll(toRemove);
				if(active.size()>maxActive.getReduceTo()){
					CompletableFuture promise=  new CompletableFuture();
					CompletableFuture.anyOf(active.toArray(new CompletableFuture[0]))
									.thenAccept(cf -> promise.complete(true));
					
					promise.join();
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.aol.simple.react.collectors.lazy.LazyResultConsumer#getResults()
	 */
	public Collection<CompletableFuture<T>> getResults(){
		return results;
	}
	/* 
	 *	@return all results (including active)
	 * @see com.aol.simple.react.collectors.lazy.LazyResultConsumer#getAllResults()
	 */
	public Collection<CompletableFuture<T>> getAllResults(){
		System.out.println("getAllResults()");
    results.addAll(active);
		return results;
	}

  @Override
  public LazyResultConsumer<T> withResults(Collection<CompletableFuture<T>> t) {
    this.results = t;
    return this;
  }

  @Override
  public MaxActive getMaxActive() {
    return maxActive;
  }

  @Override
  public ConfigurableStream<T> getBlocking() {
    return blocking;
  }
}