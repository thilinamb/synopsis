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

/**
 * Generic class for representing points in 2D or 3D space.
 *
 * @author malensek
 */
public class Point<T> {

    private T x;
    private T y;
    private T z;

    private boolean hasZ;

    /**
     * Constructs a 2D point.
     *
     * @param x X-coordinate
     * @param y Y-coordinate
     */
    public Point(T x, T y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Constructs a 3D point.
     *
     * @param x X-coordinate
     * @param y Y-coordinate
     * @param z Z-coordinate
     */
    public Point(T x, T y, T z) {
        this.x = x;
        this.y = y;
        this.z = z;

        hasZ = true;
    }

    /**
     * Reports whether this Point has a third (Z) dimension.
     *
     * @return true if a third dimension is available.
     */
    public boolean hasZ() {
        return hasZ;
    }

    public T X() {
        return x;
    }

    public T Y() {
        return y;
    }

    public T Z() {
        return z;
    }

    @Override
    public String toString() {
        if (hasZ()) {
            return "(" + x + ", " + y + ", " + z + ")";
        } else {
            return "(" + x + ", " + y + ")";
        }
    }
}
