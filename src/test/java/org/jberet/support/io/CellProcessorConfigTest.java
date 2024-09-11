/*
 * Copyright (c) 2014 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.ift.CellProcessor;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CellProcessorConfigTest {

    @Test
    public void testParseCellProcessors1() throws Exception {
        final String val = "StrMinMax(1, 20)";
        final CellProcessor[] cellProcessors = CellProcessorConfig.parseCellProcessors(val);
        System.out.printf("Resolved cell processors: %s%n", Arrays.toString(cellProcessors));
        assertEquals(1, cellProcessors.length);
        assertEquals(StrMinMax.class, cellProcessors[0].getClass());
    }

    @Test
    public void testParseCellProcessors7() throws Exception {
        final String val = "null;"
                + "Optional, StrMinMax(1, 20);"
                + "ParseLong();"
                + "NotNull, ParseInt;"
                + "ParseDate( 'dd/MM/yyyy' );"
                + "StrMinMax(1, 20);"
                + "Optional, StrMinMax(1, 20), ParseDate('dd/MM/yyyy')";
        final CellProcessor[] cellProcessors = CellProcessorConfig.parseCellProcessors(val);
        System.out.printf("Resolved cell processors: %s%n", Arrays.toString(cellProcessors));
        assertEquals(7, cellProcessors.length);
        assertEquals(null, cellProcessors[0]);
        assertEquals(Optional.class, cellProcessors[1].getClass());
        assertEquals(ParseLong.class, cellProcessors[2].getClass());
        assertEquals(NotNull.class, cellProcessors[3].getClass());
        assertEquals(ParseDate.class, cellProcessors[4].getClass());
        assertEquals(StrMinMax.class, cellProcessors[5].getClass());
        assertEquals(Optional.class, cellProcessors[6].getClass());
    }
}
