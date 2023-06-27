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

import jakarta.batch.api.BatchProperty;
import jakarta.batch.api.chunk.ItemProcessor;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@Named
public final class MovieFilterProcessor implements ItemProcessor {
    @Inject
    @BatchProperty
    private boolean filtering;

    @Override
    public Object processItem(final Object item) throws Exception {
        if (!filtering) {
            return item;
        }
        final Movie movie = (Movie) item;
        if (movie.getRating() == Movie.Rating.G) {
            return movie;
        }
        return null;
    }
}
