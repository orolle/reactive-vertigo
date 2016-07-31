# reactive-vertigo
scale-rx is a prototype to test a new architecture to provide application level scaling.

scale-rx is a framework for distributed reactive applications written with rxjava and vert.x in Java 8 for the JVM.
It provides core functions to use DHT-based scaling in your applications.
The framework contains classes to make use of DHT-capabilities within your applications easily.
* scale-node provides bootstrapping of the DHT-based network, communication and replication
* scale-data implements a reactive distributed Map to store data and react on data changes
* scale-micro-service scales and replicates micro services
* scale-business-process provides state machine 

The implementation provides
* DHT-network and routing on top of vertx: basis to to build resilient distributed reactive applications
* the serialization of java 8 functions: enables easier development for distributed algorithms, has some serialization overhead
* fully rx-ified api
