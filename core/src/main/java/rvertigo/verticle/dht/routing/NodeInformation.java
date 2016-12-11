/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.verticle.dht.routing;

import java.io.Serializable;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class NodeInformation<KEY extends Serializable & Comparable<KEY>> 
  implements Comparable<NodeInformation<KEY>>, Serializable 
{
  private KEY myself;
  private KEY next;
  private KEY previous;
  
  public KEY myself() {
    return myself;
  }
  
  public KEY next() {
    return next;
  }
  
  public KEY previous() {
    return previous;
  }

  public NodeInformation<KEY> myself(KEY keyMyself) {
    this.myself = keyMyself;
    return this;
  }

  public NodeInformation<KEY> next(KEY keyNext) {
    this.next = keyNext;
    return this;
  }

  public NodeInformation<KEY> previous(KEY keyPrevious) {
    this.previous = keyPrevious;
    return this;
  }

  @Override
  public int compareTo(NodeInformation<KEY> that) {
    return this.myself.compareTo(that.myself);
  }
}
