/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.rx;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class OutOfOrderException extends IllegalStateException {
  
  public OutOfOrderException(String msg) {
    super(msg);
  }
}
