# micro-service-rx
micro-service-rx brings Observables to vertx micro services providing reactive on-push communication to micro services.

core contains the microservicerx.rx.DistributedObservable-class which implements a simple protocol to chain 
Observable via vertx event bus with each other. DistributedObservable or its derived JsonObject can be used
as an argument and / or as a result of a micro service method-call to setup an Observable processing-pipeline 
across vertx cluster nodes.

## Example
Look into the tests of example-sub-project. Vertx 3.3.3 spawns sometimes logging errors, which are vertx internal 
but do not influence the functionality of the micro-service-rx.

## Features
* rx-ified vertx micro services via DistributedObservable
* leverages vertx-rx-java
* compatible with vertx-codegen and vertx-service-proxy
* Cold Observable
* Hot Observable
* Generic implementation of a distributed hash map for efficient message distribution
** Map-keys are any type which implements Comparable and Serializable
** Map-values are any type which implements Serializable
** Serializable-functions to customize operators / operations on the distributed hash map

## Feature up coming (in priority order)
* State machine compiler model for Observable protocol for expandability, robustness and performance
* Publish Observable which uses distributed hash map for efficient message distribution
* Back-pressure for Observables
* Monitoring of Observables

## Observables Inner Working
DistributedObservable.toDistributable(Observable, Vertx) uses an Observable and Vertx to map rxs subscribe(), onNext(), 
onError() and onCompleted() to event bus messages. DistributedObservable.toJsonObject() serializes the 
DistributedObservable and can be used as an argument and / or response of a rx-ified micro service.

The JsonObject can be deserialized back via DistributedObservable.fromJsonObject() and converted to an Observable via
DistributedObservable.toObservable(Vertx). toObservable() maps back event bus messages to subscribe(), onNext(), 
onError() and onCompleted() of an Observable and can be used like any other rx-Observable.

## Distributed Hash Map Inner Working
Distributed hash map has a logical ring topology with cross links for optimized routing. Each node within the ring 
is responsible for all its keys within his key range. His key-range is defined by his key and his neighbors key: 
key range = [node.key, neighbor.key). Through this logical structure, each node has just to maintain its connectedness
(structural integrity) to its neighbor. As long as all nodes within the ring maintaining their connectedness to 
their neighbors, the topology as a whole fulfills the structural integrity requirement.

Through this ring topology, key can be  anything Comparable and Serializable. For example if a new_node joins, within 
nodes key range: node.key < new_node.key < neighbor.key, then  nodes key range is reorganized by 
[node.key, new_node.key) and [new_node.key, neighbor). 
Reminder brackets are important: [inclusive range start, exclusive range end). 

