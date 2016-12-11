/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.verticle.dht.routing;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import rvertigo.function.Serializer;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class SerializableCodec<KLASS>
  implements MessageCodec<KLASS, KLASS> 
{
  
  public final Class<KLASS> klass;
  
  public SerializableCodec(Class<KLASS> klass) {
    this.klass = klass;
  }
  
  
  @Override
  public void encodeToWire(Buffer buffer, KLASS s) {
    byte[] data = Serializer.serialize(s);
    int length = data.length;

    // Write data into given buffer
    buffer.appendInt(length);
    buffer.appendBytes(data);
  }

  @Override
  public KLASS decodeFromWire(int position, Buffer buffer) {
    int _pos = position;
    // get length of byte[]
    int length = buffer.getInt(_pos);

    // Get JSON string by it`s length
    // Jump 4 because getInt() == 4 bytes
    byte[] data = buffer.getBytes(_pos+=4, _pos+=length);
    
    return Serializer.deserialize(data);
  }

  @Override
  public KLASS transform(KLASS s) {
    return Serializer.deserialize(Serializer.serialize(s));
  }

  @Override
  public String name() {
    return klass.getCanonicalName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}

