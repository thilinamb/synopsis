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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains a set of {@link Feature}s.
 *
 * @author malensek
 */
public class FeatureSet implements ByteSerializable, Iterable<Feature> {

    private static final Logger logger = Logger.getLogger("io.sigpipe.sing");

    private Map<String, Feature> features = new HashMap<String, Feature>();

    public FeatureSet() { }

    public void put(Feature feature) {
        features.put(feature.getName(), feature);
    }

    public Feature get(String name) {
        return features.get(name);
    }

    @Override
    public Iterator<Feature> iterator() {
        return features.values().iterator();
    }

    public int size() {
        return features.size();
    }

    public Feature[] toArray() {
        Feature[] fArray = new Feature[features.size()];
        int i = 0;
        for (Map.Entry<String, Feature> entry : features.entrySet()) {
            fArray[i++] = entry.getValue();
        }
        return fArray;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(features.values().size() + " features in FeatureSet:");
        for (Feature feature : features.values()) {
            sb.append(System.lineSeparator());
            sb.append(feature.toString());
        }
        return sb.toString();
    }

    @Deserialize
    public FeatureSet(SerializationInputStream in)
    throws IOException {
        int numFeatures = in.readInt();
        for (int i = 0; i < numFeatures; ++i) {
            Feature feature = null;
            try {
                feature = new Feature(in);
            } catch (SerializationException e) {
                logger.log(Level.WARNING, "Error deserializing FeatureSet "
                        + "element", e);
            }
            put(feature);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(features.size());
        for (Feature feature : features.values()) {
            out.writeSerializable(feature);
        }
    }
}
