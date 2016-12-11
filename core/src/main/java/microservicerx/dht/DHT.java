package microservicerx.dht;

import java.io.Serializable;
import microservicerx.function.AsyncFunction;
import microservicerx.function.Serializer;


public interface DHT {

  public static String toAddress(String prefix, Object hash) {
    final StringBuffer buf = new StringBuffer();
    buf.append(prefix).append(".").append(Integer.toHexString(hash.hashCode()));
    return buf.toString();
  }

  /*
  public static boolean isResponsible(DhtNode<?> node, Integer hash) {
    return isResponsible(node.myHash, node.nextHash, hash);
  }
  */

  public static <T extends Comparable<T>> boolean isResponsible(T rangeStart, T rangeEnd, T value) {
    // continued hash range
    if (rangeStart.compareTo(rangeEnd) < 0) {
      return rangeStart.compareTo(value) <= 0 && rangeEnd.compareTo(value) > 0;
    }
    // discontinued hash range
    else {
      return rangeStart.compareTo(value) <= 0 || rangeEnd.compareTo(value) > 0;
    }
  }

  public static <NODE, R extends Serializable> byte[] managementMessage(
    AsyncFunction<DhtLambda<NODE, R>, R> f) {
    return Serializer.serialize(f);
  }
}
