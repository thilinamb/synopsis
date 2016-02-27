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

package io.sigpipe.sing.dataset;

import java.io.IOException;

import io.sigpipe.sing.dataset.feature.FeatureSet;
import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Represents FileBlock metadata.
 *
 * @author malensek
 */
public class BlockMetadata implements ByteSerializable {

    private String name = "";

    private TemporalProperties temporalProperties;
    private SpatialProperties spatialProperties;

    private FeatureSet features = new FeatureSet();
    private DeviceSet devices = new DeviceSet();

    private RuntimeMetadata runtimeMetadata = new RuntimeMetadata();

    public BlockMetadata(String name, TemporalProperties temporalProperties,
            SpatialProperties spatialProperties,
            FeatureSet features, DeviceSet devices) {
        this(temporalProperties, spatialProperties, features, devices);
        this.name = name;
    }

    public BlockMetadata(TemporalProperties temporalProperties,
            SpatialProperties spatialProperties,
            FeatureSet features, DeviceSet devices) {

        this.temporalProperties = temporalProperties;
        this.spatialProperties = spatialProperties;
        this.features = features;
        this.devices  = devices;
    }

    public RuntimeMetadata getRuntimeMetadata() {
        return runtimeMetadata;
    }

    public void setRuntimeMetadata(RuntimeMetadata runtimeMetadata) {
        this.runtimeMetadata = runtimeMetadata;
    }

    public String getName() {
        return name;
    }

    public TemporalProperties getTemporalProperties() {
        return temporalProperties;
    }

    public SpatialProperties getSpatialProperties() {
        return spatialProperties;
    }

    public FeatureSet getFeatures() {
        return features;
    }

    public DeviceSet getDevices() {
        return devices;
    }

    @Override
    public String toString() {
        return "BlockMetadata Descriptor:" + System.lineSeparator() +
            "Name: '" + name + "'" + System.lineSeparator() +
            temporalProperties + System.lineSeparator() +
            spatialProperties + System.lineSeparator() +
            features + System.lineSeparator() +
            devices + System.lineSeparator() +
            runtimeMetadata;
    }

    @Deserialize
    public BlockMetadata(SerializationInputStream in)
    throws IOException {
        name = new String(in.readString());
        temporalProperties = new TemporalProperties(in);
        spatialProperties = new SpatialProperties(in);
        features = new FeatureSet(in);
        devices = new DeviceSet(in);
        runtimeMetadata = new RuntimeMetadata(in);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeString(name);
        out.writeSerializable(temporalProperties);
        out.writeSerializable(spatialProperties);
        out.writeSerializable(features);
        out.writeSerializable(devices);
        out.writeSerializable(runtimeMetadata);
    }
}
