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

import java.util.Arrays;

public class ByteArray implements Comparable<ByteArray> {

    private byte[] array;

    public ByteArray(byte[] bytes) {
        this.array = bytes;
    }

    public byte[] getBytes() {
        return array;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(array);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ByteArray other = (ByteArray) obj;
        if (!Arrays.equals(array, other.array)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ByteArray otherBytes) {
        int size1 = this.array.length;
        int size2 = otherBytes.array.length;

        for (int i = 0; i < size1 && i < size2; ++i) {
            int thisValue = this.array[i] & 0xFF;
            int thatValue = otherBytes.array[i] & 0xFF;

            if (thisValue != thatValue) {
                return thisValue - thatValue;
            }
        }

        /* The arrays have been the same thus far.  If they are a different
         * length then do the comparison based on that, otherwise the following
         * will return 0 (arrays are the same).  */
        return this.array.length - otherBytes.array.length;
    }
}
