/*
Copyright (c) 2013, Colorado State University
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

package io.sigpipe.sing.dataset.feature;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.serialization.SimpleMap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Maps array names (Strings) to their respective FeatureArrays.
 *
 * @author malensek
 */
public class FeatureArraySet
implements ByteSerializable, Iterable<FeatureArray>,
        SimpleMap<String, FeatureArray> {

    private Map<String, FeatureArray> arrays = new HashMap<>();

    public FeatureArraySet() { }

    @Override
    public void put(FeatureArray array) {
        arrays.put(array.getName(), array);
    }

    @Override
    public FeatureArray get(String name) {
        return arrays.get(name);
    }

    @Override
    public Iterator<FeatureArray> iterator() {
        return arrays.values().iterator();
    }

    @Override
    public Collection<FeatureArray> values() {
        return arrays.values();
    }

    @Override
    public int size() {
        return arrays.size();
    }

    @Deserialize
    public FeatureArraySet(SerializationInputStream in)
    throws IOException, SerializationException {
        in.readSimpleMap(FeatureArray.class, this);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeSimpleMap(this);
    }
}
