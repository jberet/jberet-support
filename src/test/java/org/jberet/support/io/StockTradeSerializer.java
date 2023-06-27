/*
 * Copyright (c) 2016 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.jberet.util.BatchUtil;

import jakarta.batch.operations.BatchRuntimeException;

public class StockTradeSerializer implements Serializer<StockTrade> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public byte[] serialize(final String topic, final StockTrade stockTrade) {
        try {
            return BatchUtil.objectToBytes(stockTrade);
        } catch (IOException e) {
            throw new BatchRuntimeException("Failed to serialize to topic " + topic + ", StockTrade: " + stockTrade, e);
        }
    }

    @Override
    public void close() {

    }
}
