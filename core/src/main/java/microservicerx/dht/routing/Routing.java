/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.dht.routing;

import java.io.Serializable;
import java.util.TreeSet;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class Routing<KEY extends Serializable & Comparable<KEY>> {
  private final NodeInformation<KEY> myself = new NodeInformation<>();
  TreeSet<NodeInformation<KEY>> others = new TreeSet<>();
  
  public NodeInformation<KEY> myself() {
    return myself;
  }
  
}
