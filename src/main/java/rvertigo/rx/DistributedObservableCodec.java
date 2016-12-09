/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.rx;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class DistributedObservableCodec implements MessageCodec<DistributedObservable, DistributedObservable> {

  @Override
  public void encodeToWire(Buffer buffer, DistributedObservable customMessage) {
    // Easiest ways is using JSON object
    JsonObject jsonToEncode = new JsonObject();
    jsonToEncode.put("address", customMessage.address);
    
    // Encode object to string
    String jsonToStr = jsonToEncode.encode();

    // Length of JSON: is NOT characters count
    int length = jsonToStr.getBytes().length;

    // Write data into given buffer
    buffer.appendInt(length);
    buffer.appendString(jsonToStr);
  }

  @Override
  public DistributedObservable decodeFromWire(int position, Buffer buffer) {
    // My custom message starting from this *position* of buffer
    int _pos = position;

    // Length of JSON
    int length = buffer.getInt(_pos);

    // Get JSON string by it`s length
    // Jump 4 because getInt() == 4 bytes
    String jsonStr = buffer.getString(_pos += 4, _pos += length);
    JsonObject contentJson = new JsonObject(jsonStr);

    // Get fields
    String address = contentJson.getString("address");

    // We can finally create custom message object
    return new DistributedObservable(address);
  }

  @Override
  public DistributedObservable transform(DistributedObservable customMessage) {
    // If a message is sent *locally* across the event bus.
    // This example sends message just as is
    return customMessage.clone();
  }

  @Override
  public String name() {
    // Each codec must have a unique name.
    // This is used to identify a codec when sending a message and for unregistering codecs.
    return this.getClass().getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    // Always -1
    return -1;
  }
}
