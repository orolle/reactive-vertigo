package rvertigo.verticle.dht;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class AsyncMap<K extends Serializable & Comparable<K>, V extends Serializable> implements Serializable {
  private static final long serialVersionUID = 6604642087431893642L;
  protected transient final DhtNode<K, V> node;

  protected final TreeMap<K, V> values;
  protected final TreeMap<K, Map<Integer, AsyncMapCallback<K, V>>> cbs;

  protected final Comparator<K> comparator = new Comparator<K>() {
    @Override
    public int compare(K o1, K o2) {
      return o1.compareTo(o2);
    }
  };

  public AsyncMap(DhtNode<K, V> node) {
    super();
    this.node = node;
    this.values = new TreeMap<>(comparator);
    this.cbs = new TreeMap<>(comparator);
  }

  public V get(K key) {
    return this.values.get(key);
  }

  public AsyncMap<K, V> put(K key, V value) {
    final Entry<K, V> e = new AbstractMap.SimpleEntry<>(key, value);
    this.values.put(key, value);

    if (cbs.containsKey(key)) {
      cbs.get(key).entrySet().forEach(entry -> entry.getValue().apply(new AbstractMap.SimpleEntry<>(node, e), v -> {
      }));
    }
    return this;
  }
  
  public AsyncMap<K, V> clear() {
    values.clear();
    cbs.clear();
    return this;
  }

  public Integer onChange(K key, AsyncMapCallback<K, V> cb) {
    if (!cbs.containsKey(key)) {
      cbs.put(key, new TreeMap<>());
    }
    Integer callbackKey = cb.hashCode();

    cbs.get(key).put(callbackKey, cb);

    // notify callback of current data
    cb.apply(new AbstractMap.SimpleEntry<>(node, new AbstractMap.SimpleEntry<>(key, get(key))), v -> {
    });

    return callbackKey;
  }

  public Set<Entry<K, V>> entrySet() {
    return this.values.entrySet();
  }
}
