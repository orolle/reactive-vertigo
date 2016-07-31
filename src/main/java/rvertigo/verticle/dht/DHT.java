package rvertigo.verticle.dht;

import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import org.javatuples.Pair;
import rvertigo.function.AsyncFunction;
import rvertigo.function.Serializer;


public interface DHT {

  public static String toAddress(String prefix, Integer hash) {
    final StringBuffer buf = new StringBuffer();
    buf.append(prefix).append(".").append(Integer.toHexString(hash));
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
