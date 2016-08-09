package rvertigo.verticle.dht;

import java.io.Serializable;
import java.util.Map.Entry;
import rvertigo.function.AsyncFunction;

public interface AsyncMapCallback<K extends Serializable & Comparable<K>, V extends Serializable>
  extends AsyncFunction<Entry<DhtNode<K, V>, Entry<K, V>>, Void> {

}
