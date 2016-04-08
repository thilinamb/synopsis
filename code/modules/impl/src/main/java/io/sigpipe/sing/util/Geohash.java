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

package io.sigpipe.sing.util;

import java.util.ArrayList;
import java.util.HashMap;

import io.sigpipe.sing.dataset.Coordinates;
import io.sigpipe.sing.dataset.SpatialRange;

/**
 * This class provides an implementation of the Geohash (http://www.geohash.org)
 * algorithm.
 *
 * See http://en.wikipedia.org/wiki/Geohash for implementation details.
 *
 * @author malensek
 */
public class Geohash {

    public final static byte BITS_PER_CHAR   = 5;
    public final static int  LATITUDE_RANGE  = 90;
    public final static int  LONGITUDE_RANGE = 180;

    /**
     * This character array maps integer values (array indices) to their
     * Geohash base32 alphabet equivalents.
     */
    public final static char[] charMap = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c',
        'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    /**
     * Allows lookups from a Geohash character to its integer index value.
     */
    public final static HashMap<Character, Integer> charLookupTable =
        new HashMap<Character, Integer>();


    /**
     * Initialize HashMap for character to integer lookups.
     */
    static {
        for (int i = 0; i < charMap.length; ++i) {
            charLookupTable.put(charMap[i], i);
        }
    }

    /**
     * Encode a set of {@link Coordinates} into a Geohash string.
     *
     * @param coords
     *     Coordinates to get Geohash for.
     *
     * @param precision
     *     Desired number of characters in the returned Geohash String.  More
     *     characters means more precision.
     *
     * @return Geohash string.
     */
    public static String encode(Coordinates coords, int precision) {
        return encode(coords.getLatitude(), coords.getLongitude(), precision);
    }

    /**
     * Encode {@link SpatialRange} into a Geohash string.
     *
     * @param range
     *     SpatialRange to get Geohash for.
     *
     * @param precision
     *     Number of characters in the returned Geohash String.
     *     More characters is more precise.
     *
     * @return Geohash string.
     */
    public static String encode(SpatialRange range, int precision) {
        Coordinates rangeCoords = range.getCenterPoint();
        return encode(rangeCoords.getLatitude(),
                      rangeCoords.getLongitude(),
                      precision);
    }

    /**
     * Encode latitude and longitude into a Geohash string.
     *
     * @param latitude
     *     Latitude coordinate, in degrees.
     *
     * @param longitude
     *     Longitude coordinate, in degrees.
     *
     * @param precision
     *     Number of characters in the returned Geohash String.
     *     More characters is more precise.
     *
     * @return resulting Geohash String.
     */
    public static String encode(float latitude, float longitude,
                                int precision) {
        /* Set up 2-element arrays for longitude and latitude that we can flip
         * between while encoding */
        float[] high  = new float[2];
        float[] low   = new float[2];
        float[] value = new float[2];

        high[0] = LONGITUDE_RANGE;
        high[1] = LATITUDE_RANGE;
        low[0]  = - LONGITUDE_RANGE;
        low[1]  = - LATITUDE_RANGE;
        value[0] = longitude;
        value[1] = latitude;

        String hash = "";

        for (int p = 0; p < precision; ++p) {

            float middle = 0.0f;
            int charBits = 0;
            for (int b = 0; b < BITS_PER_CHAR; ++b) {
                int bit = (p * BITS_PER_CHAR) + b;

                charBits <<= 1;

                middle = (high[bit % 2] + low[bit % 2]) / 2;
                if (value[bit % 2] > middle) {
                    charBits |= 1;
                    low[bit % 2] = middle;
                } else {
                    high[bit % 2] = middle;
                }
            }

            hash += charMap[charBits];
        }

        return hash;
    }

    /**
     * Convert a Geohash String to a long integer. Sets of Geohash bits are
     * placed such that the least-significant 5 bits contain the first character
     * of the hash, and so on. This method supports up to 12 characters of
     * precision; extra characters will be dropped when creating the long
     * integer.
     *
     * @param hash
     *     Geohash String to convert.
     *
     * @return The Geohash as a long integer.
     */
    public static long hashToLong(String hash) {
        /* Long can fit 12 Geohash characters worth of precision. */
        int hashLen = hash.length();
        if (hashLen > 12) {
            hash = hash.substring(0, 12);
        }

        long longHash = 0;
        for (int i = hashLen - 1; i >= 0; --i) {
            char c = hash.charAt(i);
            longHash |= charLookupTable.get(c);

            if (i > 0) {
                longHash <<= BITS_PER_CHAR;
            }
        }

        return longHash;
    }

    /**
     * Decode a Geohash to an approximate bounding box that contains the
     * original Geohashed point.
     *
     * @param geoHash
     *     Geohash string
     *
     * @return Spatial Range (bounding box) of the Geohash.
     */
    public static SpatialRange decodeHash(String geoHash) {
        ArrayList<Boolean> bits = getBits(geoHash);

        float[] longitude = decodeBits(bits, false);
        float[] latitude  = decodeBits(bits, true);

        return new SpatialRange(latitude[0], latitude[1],
                                    longitude[0], longitude[1]);
    }

    /**
     * Decode Geohash bits from a binary Geohash.
     *
     * @param bits
     *     ArrayList of Booleans containing the Geohash bits
     *
     * @param latitude
     *     If set to <code>true</code> the latitude bits are decoded.  If set to
     *     <code>false</code> the longitude bits are decoded.
     *
     * @return low, high range that the Geohashed location falls between.
     */
    private static float[] decodeBits(ArrayList<Boolean> bits,
                                      boolean latitude) {
        float low, high, middle;
        int offset;

        if (latitude) {
            offset = 1;
            low    = -90.0f;
            high   =  90.0f;
        } else {
            offset = 0;
            low    = -180.0f;
            high   =  180.0f;
        }

        for (int i = offset; i < bits.size(); i += 2) {
            middle = (high + low) / 2;

            if (bits.get(i)) {
                low  = middle;
            } else {
                high = middle;
            }
        }

        if (latitude) {
            return new float[] { high, low };
        } else {
            return new float[] { low, high };
        }
    }

    /**
     * Converts a Geohash string to its binary representation.
     *
     * @param hash
     *     Geohash string to convert to binary
     *
     * @return The Geohash in binary form, as an ArrayList of Booleans.
     */
    private static ArrayList<Boolean> getBits(String hash) {
        hash = hash.toLowerCase();

        /* Create an array of bits, 5 bits per character: */
        ArrayList<Boolean> bits =
            new ArrayList<Boolean>(hash.length() * BITS_PER_CHAR);

        /* Loop through the hash string, setting appropriate bits. */
        for (int i = 0; i < hash.length(); ++i) {
            int charValue = charLookupTable.get(hash.charAt(i));

            /* Set bit from charValue, then shift over to the next bit. */
            for (int j = 0; j < BITS_PER_CHAR; ++j, charValue <<= 1) {
                bits.add((charValue & 0x10) == 0x10);
            }
        }
        return bits;
    }
}
