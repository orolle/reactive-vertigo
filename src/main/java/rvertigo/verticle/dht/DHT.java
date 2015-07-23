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

  public static boolean isResponsible(Integer my, Integer next, Integer hash) {
    // continued hash range
    if (my < next) {
      return my <= hash && next > hash;
    }
    // discontinued hash range
    else {
      return my <= hash || next > hash;
    }
  }

  public static <T extends Serializable, R extends Serializable> byte[] managementMessage(
    AsyncFunction<Pair<ReactiveLambda<DhtNode<T>, Message<byte[]>, R>, Message<byte[]>>, R> f) {
    return Serializer.serialize(f);
  }
}
