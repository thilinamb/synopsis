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

package io.sigpipe.sing.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.graph.Vertex;
import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * General query interface. In SING, queries are executed against a graph
 * (defined by its root vertex).
 *
 * @author malensek
 */
public abstract class Query implements ByteSerializable {

    protected Map<String, List<Expression>> expressions = new HashMap<>();

    public Query() {

    }

    public abstract void execute(Vertex root)
    throws IOException, QueryException;

    public void addExpression(Expression e) {
        String name = e.getOperand().getName();
        List<Expression> expList = expressions.get(name);
        if (expList == null) {
            expList = new ArrayList<>();
            expressions.put(name, expList);
        }
        expList.add(e);
    }

    protected Set<Vertex> evaluate(Vertex vertex, List<Expression> expressions)
    throws QueryException {
        Set<Vertex> matches = new HashSet<>(vertex.numNeighbors(), 1.0f);

        for (Expression expression : expressions) {

            Operator operator = expression.getOperator();
            Feature operand = expression.getOperand();

            switch (operator) {
                case EQUAL: {
                    matches.add(vertex.getNeighbor(operand));
                    break;
                }

                case NOTEQUAL: {
                    boolean exists = matches.contains(operand);
                    matches.addAll(vertex.getAllNeighbors());
                    if (exists == false) {
                        /* If the operand (not equal value) wasn't already added
                         * by another expression, we can safely remove it now.
                         * In other words, if another expression includes the
                         * value excluded by this expression, the user has
                         * effectively requested the entire neighbor set. */
                        matches.remove(operand);
                    }
                    break;
                }

                case LESS: {
                    matches.addAll(
                            vertex.getNeighborsLessThan(operand, false)
                            .values());
                    break;
                }

                case LESSEQUAL: {
                    matches.addAll(
                            vertex.getNeighborsLessThan(operand, true)
                            .values());
                    break;
                }

                case GREATER: {
                    matches.addAll(
                            vertex.getNeighborsGreaterThan(operand, false)
                            .values());
                    break;
                }

                case GREATEREQUAL: {
                    matches.addAll(
                            vertex.getNeighborsGreaterThan(operand, true)
                            .values());
                    break;
                }

                case RANGE_INC: {
                    Feature secondOperand = expression.getSecondOperand();
                    matches.addAll(vertex.getNeighborsInRange(
                                operand, true,
                                secondOperand, true)
                            .values());
                    break;
                }

                case RANGE_EXC: {
                    Feature secondOperand = expression.getSecondOperand();
                    matches.addAll(vertex.getNeighborsInRange(
                                operand, false,
                                secondOperand, false)
                            .values());
                    break;
                }

                case RANGE_INC_EXC: {
                    Feature secondOperand = expression.getSecondOperand();
                    matches.addAll(vertex.getNeighborsInRange(
                                operand, true,
                                secondOperand, false)
                            .values());
                    break;
                }

                case RANGE_EXC_INC: {
                    Feature secondOperand = expression.getSecondOperand();
                    matches.addAll(vertex.getNeighborsInRange(
                                operand, false,
                                secondOperand, true)
                            .values());
                    break;
                }

                case STR_PREFIX: {
                    vertex
                        .getAllNeighbors()
                        .stream()
                        .filter(v -> v
                                .getLabel()
                                .getString()
                                .startsWith(operand.getString()))
                        .forEach(matches::add);
                    break;
                }

                case STR_SUFFIX: {
                    vertex
                        .getAllNeighbors()
                        .stream()
                        .filter(v -> v
                                .getLabel()
                                .getString()
                                .endsWith(operand.getString()))
                        .forEach(matches::add);
                    break;
                }

                default:
                    throw new QueryException("Unknown operator: " + operator);
            }
        }

        return matches;
    }

    @Deserialize
    public Query(SerializationInputStream in)
    throws IOException, SerializationException {
        int size = in.readInt();
        this.expressions = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            int listSize = in.readInt();
            List<Expression> expList = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; ++j) {
                Expression exp = new Expression(in);
                expList.add(exp);
            }
            String featureName = expList.get(0).getOperand().getName();
            this.expressions.put(featureName, expList);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(this.expressions.size());
        for (List<Expression> expList : this.expressions.values()) {
            out.writeInt(expList.size());
            for (Expression expression : expList) {
                expression.serialize(out);
            }
        }
    }
}
