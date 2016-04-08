/*
Copyright (c) 2016, Colorado State University
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

package io.sigpipe.sing.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Simple utility that consumes a list of Geohash strings and outputs their
 * respective binary forms. The command line version receives the Geohash
 * strings from stdin.
 *
 * @author malensek
 */
public class GeoBits {

    public static void main(String[] args) throws Exception {

        InputStreamReader in = new InputStreamReader(System.in);
        try (BufferedReader br = new BufferedReader(in)) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(GeoBits.geohashToBinString(line));
            }
        }
    }

    public static String geohashToBinString(String geohash) {
        long l = Geohash.hashToLong(geohash);
        return Long.toBinaryString(l);
    }

}
