/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rvertigo.function;

import java.io.Serializable;
import rx.functions.Func2;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public interface SerializableFunc2<TYPE> extends Func2<TYPE, TYPE, TYPE>, Serializable {
  
}
