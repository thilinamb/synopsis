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

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Contains a set of {@link Device} descriptors.
 *
 * @author malensek
 */
public class DeviceSet implements ByteSerializable, Iterable<Device> {

    private Map<String, Device> devices = new HashMap<String, Device>();

    public DeviceSet() { }

    public void put(Device device) {
        devices.put(device.getName(), device);
    }

    public Device get(String name) {
        return devices.get(name);
    }

    @Override
    public Iterator<Device> iterator() {
        return devices.values().iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(devices.values().size() + " features in DeviceSet:");
        for (Device device : devices.values()) {
            sb.append(System.lineSeparator());
            sb.append(device.toString());
        }
        return sb.toString();
    }

    @Deserialize
    public DeviceSet(SerializationInputStream in)
    throws IOException {
        int numDevices = in.readInt();
        for (int i = 0; i < numDevices; ++i) {
            Device device = new Device(in);
            put(device);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(devices.size());
        for (Device device : devices.values()) {
            out.writeSerializable(device);
        }
    }
}
