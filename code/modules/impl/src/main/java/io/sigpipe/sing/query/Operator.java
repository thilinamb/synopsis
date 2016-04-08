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

package io.sigpipe.sing.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines supported relational operators for queries.
 *
 * @author malensek
 */
public enum Operator {
    UNKNOWN(0),
    EQUAL(1),
    NOTEQUAL(2),
    LESS(3),
    GREATER(4),
    LESSEQUAL(5),
    GREATEREQUAL(6),
    RANGE_INC(7),
    RANGE_EXC(8),
    RANGE_INC_EXC(9),
    RANGE_EXC_INC(10),
    STR_PREFIX(11),
    STR_SUFFIX(11),
    ;

    /**
     * String representation of the operators.  Note that the array index
     * corresponds to the operator index.  '=' and '/=' for equal and not equal,
     * respectively, are also allowed (see the strToOp Map).
     */
    private static final String[] opStrings = {
        "??", // UNKNOWN(0)
        "==", // EQUAL(1)
        "!=", // NOTEQUAL(2)
        "<",  // LESS(3)
        ">",  // GREATER(4)
        "<=", // LESSEQUAL(5)
        ">=", // GREATEREQUAL(6)
        "[]", // RANGE_INC(7)
        "()", // RANGE_EXC(8)
        "[)", // RANGE_INC_EXC(9)
        "(]", // RANGE_EXC_INC(10)
        "[=", // STR_PREFIX(11)
        "=]", // STR_SUFFIX(11)
    };

    private final int op;

    private Operator(int op) {
        this.op = op;
    }

    public int toInt() {
        return op;
    }

    static Map<Integer, Operator> opNums = new HashMap<>();

    static {
        for (Operator o : Operator.values()) {
            opNums.put(o.toInt(), o);
        }
    }

    public static Operator fromInt(int i) {
        Operator o = opNums.get(i);
        if (o == null) {
            return Operator.UNKNOWN;
        }

        return o;
    }

    static Map<String, Operator> strToOp = new HashMap<>();

    static {
        for (Operator o : Operator.values()) {
            strToOp.put(opStrings[o.toInt()], o);
        }

        strToOp.put("=", EQUAL);
        strToOp.put("/=", NOTEQUAL);
    }

    public static Operator fromString(String s) {
        Operator o = strToOp.get(s);
        if (s == null) {
            return Operator.UNKNOWN;
        }

        return o;
    }

    @Override
    public String toString() {
        return opStrings[this.op];
    }
}
