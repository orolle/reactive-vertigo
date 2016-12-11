package microservicerx.function;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface Serializer {
  public static final byte[] EMPTY = new byte[] {};

  public static byte[] serializeAsyncFunction(AsyncFunction<?, ?> f) {
    return serialize(f);
  }
  
  public static byte[] serialize(Object o) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GZIPOutputStream gos = new GZIPOutputStream(bos);
      ObjectOutputStream out = new ObjectOutputStream(gos);
      out.writeObject(o);

      out.close();
      gos.close();
      bos.close();
      
      return bos.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException("Likely cause: reference to non-serializable outer class?", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] data) {
    T result = null;
      
    try {  
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      GZIPInputStream gis = new GZIPInputStream(bis);
      ObjectInputStream in = new ObjectInputStream(gis);

      result = (T) in.readObject();
      
      in.close();
      gis.close();
      bis.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
    return result;
  }


}
