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

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Defines spatial attributes for an object in Galileo.  This may include a
 * point, rectangle, polygon, etc.
 *
 * @author malensek
 */
public class SpatialProperties implements ByteSerializable {

    private Coordinates coords;
    private SpatialRange range;

    public SpatialProperties(Coordinates coords) {
        this.coords = coords;
    }

    public SpatialProperties(float latitude, float longitude) {
        this.coords = new Coordinates(latitude, longitude);
    }

    public SpatialProperties(SpatialRange range) {
        this.range = range;
    }

    public Coordinates getCoordinates() {
        return coords;
    }

    public SpatialRange getSpatialRange() {
        return range;
    }

    public boolean hasRange() {
        return has(range);
    }

    public boolean hasCoordinates() {
        return has(coords);
    }

    /**
     * Used to determine whether this SpatialProperties instance has particular
     * properties.
     *
     * @param obj Object to test for.
     */
    private boolean has(Object obj) {
        if (obj != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (hasRange()) {
            sb.append("Spatial Range: " + getSpatialRange());
        }

        if (hasRange() && hasCoordinates()) {
            sb.append(System.lineSeparator());
        }

        if (hasCoordinates()) {
            sb.append("Coordinates: " + getCoordinates());
        }

        return sb.toString();
    }

    @Deserialize
    public SpatialProperties(SerializationInputStream in)
    throws IOException {
        boolean hasRange = in.readBoolean();
        boolean hasCoordinates = in.readBoolean();

        if (hasRange) {
            range = new SpatialRange(in);
        }

        if (hasCoordinates) {
            coords = new Coordinates(in);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        boolean hasRange = hasRange();
        boolean hasCoordinates = hasCoordinates();
        out.writeBoolean(hasRange);
        out.writeBoolean(hasCoordinates);

        if (hasRange) {
            out.writeSerializable(range);
        }

        if (hasCoordinates) {
            out.writeSerializable(coords);
        }
    }
}
