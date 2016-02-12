# reactive-vertigo
reactive-vertigo is a prototype to test a new architecture to port vertigo 0.7 (vertx2) to vertx3.

reactive-vertigo is a framework for asynchronous distributed event processing for vertx3.
It will support a variatey of concepts, including java8's streams, as well as flow based programming.
It uses reactive-streams to manage data flows for event processing. 
It uses an distributed hash table as an in-memory data store. 
In future it will support other data distribution technologies, like copycat.


This prototype prooves that
* the serialization of java 8 functions: enables easier development for distributed algorithms, has some serialization overhead
* reactive-streams as a future standard to manage the data flow in clusters: one widely-know api to manage data flows
* a DHT can be used on top of vertx: enables resilient distributed storage, needed for event processing
* asynchronous functions and data structures are used for stream processing: this saves cpu cycles
* internally there is no need to serialize to JSON: faster

Supported of java 8 streaming api for distributed event processing:
* map
* forEach

