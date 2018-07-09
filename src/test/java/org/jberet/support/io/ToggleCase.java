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

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.util.CsvContext;

public final class ToggleCase extends CellProcessorAdaptor {
    private static String upper = "A";
    private static String lower = "a";
    private String whichCase = upper;

    public ToggleCase() {
        super();
    }

    public ToggleCase(final CellProcessor next) {
        super(next);
    }

    public ToggleCase(final String whichCase) {
        this.whichCase = whichCase;
    }

    public ToggleCase(final CellProcessor next, final String whichCase) {
        super(next);
        this.whichCase = whichCase;
    }

    @Override
    public Object execute(final Object value, final CsvContext context) {
        validateInputNotNull(value, context);
        final String s = (String) value;
        if (whichCase.equals(lower)) {
            return next.execute(s.toLowerCase(), context);
        }
        return next.execute(s.toUpperCase(), context);
    }
}
