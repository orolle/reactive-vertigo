package rvertigo.verticle.dht;

import java.io.Serializable;
import java.util.Map.Entry;

import org.javatuples.Pair;

import rvertigo.function.AsyncFunction;

public interface AsyncMapCallback<K extends Comparable<K>, V extends Serializable>
  extends AsyncFunction<Pair<DhtNode<V>, Entry<K, V>>, Void> {

}
