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

import java.util.Date;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.ByteSerializable.Deserialize;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class TemporalProperties implements ByteSerializable {

    private long start;
    private long end;

    private DateFormat formatter = DateFormat.getDateInstance(DateFormat.LONG);

    /**
     * Creates TemporalProperties with a simple timestamp (no temporal range).
     *
     * @param timestamp Timestamp for these TemporalProperties
     */
    public TemporalProperties(long timestamp) {
        this.start = timestamp;
        this.end = timestamp;
    }

    /**
     * Creates a simple temporal range from a start and end time pair,
     * represented in miliseconds from the Unix epoch.
     *
     * @param start Start of the temporal range
     * @param end   End of the temporal range
     */
    public TemporalProperties(long start, long end)
    throws IllegalArgumentException {
        this.start = start;
        this.end = end;

        verifyRange();
    }

    /**
     * Creates a simple temporal range from a start and end time pair,
     * represented as Strings.
     *
     * @param start Start of the temporal range
     * @param end   End of the temporal range
     */
    public TemporalProperties(String start, String end)
    throws ParseException, IllegalArgumentException {
        this(0, 0);
        Date startDate = formatter.parse(start);
        Date endDate = formatter.parse(end);

        this.start = startDate.getTime();
        this.end = endDate.getTime();

        verifyRange();
    }

    /**
     * Ensure the start time comes before the end time for this Temporal range.
     */
    private void verifyRange() throws IllegalArgumentException {
        if (getEnd() - getStart() <= 0) {
            throw new IllegalArgumentException("Upper bound of temporal range" +
                " must be larger than the lower bound.");
        }
    }

    /**
     * Retrieves the lower bound of this temporal range (if applicable).
     */
    public Date getLowerBound() {
        return new Date(getStart());
    }

    /**
     * Retrieves the upper bound of this temporal range (if applicable).
     */
    public Date getUpperBound() {
        return new Date(getEnd());
    }

    /**
     * Get the starting point of the time interval represented by this
     * TemporalProperties instance.
     * 
     * @return starting point, as a long integer.
     */
    public long getStart() {
        return start;
    }

    /**
     * Get the end point of the time interval represented by this
     * TemporalProperties instance.
     *
     * @return the end time point, as a long integer.
     */
    public long getEnd() {
        return end;
    }

    /** 
     * If the temporal range stored in these TemporalProperties is of length
     * zero (i.e., start == end) then it is considered a Timestamp.  To retrieve
     * the value of a Timestamp, both the upper and lower bounds are valid.
     *
     * @return true if these TemporalProperties represent a Timestamp.
     */
    public boolean isTimestamp() {
        if (getStart() == getEnd()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        if (isTimestamp()) {
            return "Timestamp: " + getStart();
        } else {
            return "Temporal Range: " + getStart() + " -- " + getEnd();
        }
    }

    @Deserialize
    public TemporalProperties(SerializationInputStream in)
    throws IOException {
        this.start = in.readLong();
        this.end = in.readLong();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeLong(this.start);
        out.writeLong(this.end);
    }
}
