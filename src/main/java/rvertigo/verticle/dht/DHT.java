package rvertigo.verticle.dht;

import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import org.javatuples.Pair;
import rvertigo.function.AsyncFunction;
import rvertigo.function.ReactiveLambda;
import rvertigo.function.Serializer;


public interface DHT {

  public static String toAddress(String prefix, Integer hash) {
    final StringBuffer buf = new StringBuffer();
    buf.append(prefix).append(".").append(Integer.toHexString(hash));
    return buf.toString();
  }

  public static boolean isResponsible(DhtNode<?> node, Integer hash) {
    return isResponsible(node.myHash, node.nextHash, hash);
  }

  public static boolean isResponsible(Integer rangeStart, Integer rangeEnd, Integer value) {
    // continued hash range
    if (rangeStart < rangeEnd) {
      return rangeStart <= value && rangeEnd >= value;
    }
    // discontinued hash range
    else {
      return rangeStart <= value || rangeEnd >= value;
    }
  }

  public static <T extends Serializable, R extends Serializable> byte[] managementMessage(
    AsyncFunction<ReactiveLambda<Pair<DhtNode<T>, Message<byte[]>>, Message<byte[]>, R>, R> f) {
    return Serializer.serialize(f);
  }
}
