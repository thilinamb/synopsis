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
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A very simple performance timer implementation using System.nanoTime().
 *
 * This class can record multiple samples (time intervals), print them, etc.
 *
 * @author malensek
 */
public class PerformanceTimer {

    /** The name of this {@link PerformanceTimer} */
    protected String name = "";

    /** Timer samples (time intervals) */
    protected Deque<PerformanceSample> samples = new LinkedList<>();

    /**
     * Records information about an interval of time as a performance sample.
     */
    public class PerformanceSample {
        public boolean running = false;
        public long start;
        public long end;

        @Override
        public String toString() {
            String str = "" + timeInMs();
            if (running) {
                str += " (running)";
            }
            return str;
        }

        public double timeInMs() {
            return ((end - start) * 1E-6);
        }
    }

    /**
     * Create a new nameless PerformanceTimer.
     */
    public PerformanceTimer() {
        samples.addFirst(new PerformanceSample());
    }

    /**
     * Create a named PerformanceTimer.
     */
    public PerformanceTimer(String name) {
        this();
        this.name = name;
    }

    /**
     * Starts a time interval.
     */
    public void start() {
        PerformanceSample sample = samples.peekFirst();
        sample.running = true;
        sample.start = System.nanoTime();
    }

    /**
     * Stops the currently-running time interval.
     */
    public void stop() {
        PerformanceSample sample = samples.peekFirst();
        sample.end = System.nanoTime();
        sample.running = false;
        samples.addFirst(new PerformanceSample());
    }

    /**
     * Stops the currently-running time interval and prints its duration.
     */
    public void stopAndPrint() {
        PerformanceSample sample = samples.peekFirst();
        stop();
        System.out.println(name + "=" + sample.toString());
    }

    /**
     * Get all the samples recorded by this PerformanceTimer so far.
     *
     * @return PerformanceSamples recorded thus far.
     */
    public List<PerformanceSample> getSamples() {
        List<PerformanceSample> outputSamples = new ArrayList<>();
        for (PerformanceSample sample : samples) {
            outputSamples.add(sample);
        }
        /* Remove the "current" sample from the output list */
        outputSamples.remove(0);

        /* Order chronologically */
        Collections.reverse(outputSamples);

        return outputSamples;
    }

    /**
     * Retrieves the last timing result, in miliseconds.  If no results have
     * been recorded yet, this method will return 0.0.
     */
    public double getLastResult() {
        Iterator<PerformanceSample> it = samples.iterator();
        it.next();
        if (it.hasNext()) {
            return it.next().timeInMs();
        } else {
            return 0.0;
        }
    }

    /**
     * Retrieves the number of samples recorded by this PerformanceTimer
     * instance.
     */
    public int size() {
        /* Ignore the head of the queue; this contains a currently-running (or
         * empty) sample. */
        return samples.size() - 1;
    }

    @Override
    public String toString() {
        String str = "";
        for (PerformanceSample sample : samples) {
            str += name + "=" + sample + System.lineSeparator();
        }
        return str;
    }
}
