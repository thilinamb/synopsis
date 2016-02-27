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

import io.sigpipe.sing.dataset.feature.Feature;

public class Temperature extends Feature {

    public Temperature(String name, double temp) {
        super(name, temp);
    }

    public Temperature(double temp) {
        super(temp);
    }

    public double heatIndex(double relativeHumidity) {
        double t = this.getDouble();
        double t2 = Math.pow(t, 2);
        double h = relativeHumidity;
        double h2 = Math.pow(h, 2);
        return -42.379 + (2.04901523 * t) + (10.14333127 * h)
            - (0.22475541 * t * h) - (6.83783 * 0.001 * t2)
            - (5.481717 * 0.01 * h2) + (1.22874 * 0.001 * t2 * h)
            + (8.5282 * 0.0001 * t * h2) - (1.99 * 1E-6 * t2 * h2);

    }
}
