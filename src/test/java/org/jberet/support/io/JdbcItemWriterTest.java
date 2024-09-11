/*
 * Copyright (c) 2015 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import static org.jberet.support.io.JdbcItemWriter.determineParameterNames;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.batch.operations.BatchRuntimeException;
import org.junit.jupiter.api.Test;

@SuppressWarnings("SpellCheckingInspection")
public class JdbcItemWriterTest {

    @Test
    public void normal() throws Exception {
        final String sql = "INSERT INTO forex (symbol, ts, bid_open, bid_high, bid_low, bid_close, volume) " +
                "values ('USDJPY', ?, ?, ?, ?, ?, ?)";
        final String[] actual = determineParameterNames(sql);
        final String[] expected = {"ts", "bid_open", "bid_high", "bid_low", "bid_close", "volume"};
        assertArrayEquals(expected, actual);
    }

    @Test
    public void canNotDetermine() throws Exception {
        assertThrows(BatchRuntimeException.class, () -> {
            final String sql = "INSERT INTO forex (symbol, ts, bid_open, bid_high, bid_low, bid_close, volume) " +
                    "values ('USDJPY', parsedatetime('yyyyMMdd HHmmss', ?), ?, ?, ?, ?, ?)";
            determineParameterNames(sql);
        });
    }
}
