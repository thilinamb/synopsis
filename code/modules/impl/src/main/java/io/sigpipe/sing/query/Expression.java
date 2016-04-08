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

import java.io.IOException;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Representation of a query expression.  For example: x != 3.6.  Contains an
 * operator, and an associated value (or values in the case of range operators).
 *
 * @author malensek
 */
public class Expression implements ByteSerializable {

    private Operator operator;
    private Feature operand1;
    private Feature operand2;

    public Expression(Operator operator, Feature operand) {
        this.operator = operator;
        this.operand1 = operand;
    }

    public Expression(String operator, Feature operand) {
        this(Operator.fromString(operator), operand);
    }

    public Expression(Operator operator, Feature start, Feature end) {
        if (operator != Operator.RANGE_EXC
                && operator != Operator.RANGE_INC
                && operator != Operator.RANGE_EXC_INC
                && operator != Operator.RANGE_INC_EXC) {

            throw new IllegalArgumentException(
                    "Range-based expression requires a range operator");
        }

        this.operator = operator;
        this.operand1 = start;
        this.operand2 = end;
    }

    public Expression(String operator, Feature start, Feature end) {
        this(Operator.fromString(operator), start, end);
    }

    public Operator getOperator() {
        return operator;
    }

    public Feature getOperand() {
        return this.operand1;
    }

    public Feature getSecondOperand() {
        return this.operand2;
    }

    @Override
    public String toString() {
        switch (operator) {
            case RANGE_INC:
            case RANGE_EXC:
            case RANGE_INC_EXC:
            case RANGE_EXC_INC:
                return operand1.getName() + " in "
                    + operator.toString().charAt(0)
                    + operand1.getString()
                    + ", "
                    + operand2.getString()
                    + operator.toString().charAt(1);

            default:
                return operand1.getName() + " " + operator + " "
                    + operand1.getString();
        }
    }

    @Deserialize
    public Expression(SerializationInputStream in)
    throws IOException, SerializationException {
        operator = Operator.fromInt(in.readInt());
        boolean hasRange = (in.readInt() == 2);
        this.operand1 = new Feature(in);
        if (hasRange) {
            this.operand2 = new Feature(in);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(operator.toInt());
        int numOperands = (operand2 != null) ? 2 : 1;
        out.writeInt(numOperands);
        operand1.serialize(out);
        if (operand2 != null) {
            operand2.serialize(out);
        }
    }
}
