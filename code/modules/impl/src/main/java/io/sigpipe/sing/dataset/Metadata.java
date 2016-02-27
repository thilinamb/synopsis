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

package io.sigpipe.sing.dataset;

import java.io.IOException;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureArray;
import io.sigpipe.sing.dataset.feature.FeatureArraySet;
import io.sigpipe.sing.dataset.feature.FeatureSet;
import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class Metadata implements ByteSerializable {

    private String name = "";

    /**
     * Metadata attributes: these Features are represented by a 1D array and
     * are accessed as a simple key-value store.
     */
    private FeatureSet attributes = new FeatureSet();

    /**
     * A key-value store for multidimensional {@link FeatureArray}s.
     */
    private FeatureArraySet features = new FeatureArraySet();

    /**
     * Spatial information associated with this Metadata
     */
    private SpatialProperties spatialProperties = null;

    /**
     * Temporal information associated with this Metadata
     */
    private TemporalProperties temporalProperties = null;

    /**
     * Maintains metadata information that is only valid at system run time.
     */
    private RuntimeMetadata runtimeMetadata = new RuntimeMetadata();

    /**
     * Creates an unnamed Metadata instance
     */
    public Metadata() { }

    /**
     * Creates a named Metadata instance.
     */
    public Metadata(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name == null) {
            this.name = "";
        } else {
            this.name = name;
        }
    }

    /**
     * Places a single feature into this Metadata instance's attribute
     * FeatureSet.
     */
    public void putAttribute(Feature feature) {
        attributes.put(feature);
    }

    public Feature getAttribute(String featureName) {
        return attributes.get(featureName);
    }

    public FeatureSet getAttributes() {
        return attributes;
    }

    /**
     * Sets this Metadata container's attribute FeatureSet.  This will eliminate
     * any previously-added attributes.
     *
     * @param attributes {@link FeatureSet} containing attributes that should be
     * associated with this Metadata instance.
     */
    public void setAttributes(FeatureSet attributes) {
        this.attributes = attributes;
    }

    public void putFeature(FeatureArray feature) {
        features.put(feature);
    }

    public FeatureArray getFeature(String featureName) {
        return features.get(featureName);
    }

    /**
     * Sets this Metadata container's set of Feature arrays.  This will
     * eliminate any previously-added Feature arrays.
     *
     * @param features {@link FeatureArraySet} containing features that should
     * be associated with this Metadata instance.
     */
    public void setFeatures(FeatureArraySet features) {
        this.features = features;
    }

    public FeatureArraySet getFeatures() {
        return features;
    }

    public void setSpatialProperties(SpatialProperties spatialProperties) {
        this.spatialProperties = spatialProperties;
    }

    public SpatialProperties getSpatialProperties() {
        return this.spatialProperties;
    }

    public boolean hasSpatialProperties() {
        return this.spatialProperties != null;
    }

    public void setTemporalProperties(TemporalProperties temporalProperties) {
        this.temporalProperties = temporalProperties;
    }

    public TemporalProperties getTemporalProperties() {
        return this.temporalProperties;
    }

    public boolean hasTemporalProperties() {
        return this.temporalProperties != null;
    }

    @Override
    public String toString() {
        String nl = System.lineSeparator();
        String str = "Name: '" + name + "'" + nl
            + "Contains Temporal Data: " + hasTemporalProperties() + nl;
        if (hasTemporalProperties()) {
            str += "Temporal Data Block:" + nl
            + temporalProperties.toString() + nl;
        }

        str += "Contains Spatial Data: " + hasSpatialProperties() + nl;
        if (hasSpatialProperties()) {
            str += "Spatial Data Block:" + nl
            + spatialProperties.toString() + nl;
        }

        str += "Number of Attributes: " + attributes.size() + nl;
        for (Feature f : attributes) {
            str += f.toString() + nl;
        }

        str += "Number of ND Feature Arrays: " + features.size() + nl;

        return str;
    }

    @Deserialize
    public Metadata(SerializationInputStream in)
    throws IOException, SerializationException {
        name = in.readString();

        boolean temporal = in.readBoolean();
        if (temporal) {
            temporalProperties = new TemporalProperties(in);
        }

        boolean spatial = in.readBoolean();
        if (spatial) {
            spatialProperties = new SpatialProperties(in);
        }

        attributes = new FeatureSet(in);
        features = new FeatureArraySet(in);
        runtimeMetadata = new RuntimeMetadata(in);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeString(name);

        out.writeBoolean(hasTemporalProperties());
        if (hasTemporalProperties()) {
            out.writeSerializable(temporalProperties);
        }

        out.writeBoolean(hasSpatialProperties());
        if (hasSpatialProperties()) {
            out.writeSerializable(spatialProperties);
        }

        out.writeSerializable(attributes);
        out.writeSerializable(features);
        out.writeSerializable(runtimeMetadata);
    }
}
