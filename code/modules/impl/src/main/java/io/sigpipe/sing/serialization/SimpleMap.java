/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package io.sigpipe.sing.serialization;

import io.sigpipe.sing.serialization.ByteSerializable;

import java.util.Collection;

/**
 * Defines a very basic serializable map that consists of Key, Value pairs
 * wherein the Key can be ascertained by the Value directly. For example, a
 * class that has a 'name' attribute that we wish to use as a lookup key.
 *
 * @author malensek
 */
public interface SimpleMap<K, V extends ByteSerializable> {

    /**
     * Places an item in this data structure.
     */
    public void put(V item);

    /**
     * Retrieves an item from this data structure.
     *
     * @param key Key of the item to retrieve; for instance, the item's name.
     */
    public V get(K key);

    /**
     * Retrieves all the values contained in this data structure.
     */
    public Collection<V> values();

    /**
     * Reports the current size of the data structure.
     */
    public int size();
}
