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

import java.io.Serializable;
import java.util.List;
import javax.batch.api.chunk.ItemWriter;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

/**
 * An implementation of {@code javax.batch.api.chunk.ItemWriter} that writes to a collection in a MongoDB database.
 *
 * @see     MongoItemReaderWriterBase
 * @see     MongoItemReader
 * @since   1.0.2
 */
@Named
@Dependent
public class MongoItemWriter extends MongoItemReaderWriterBase implements ItemWriter {
    @Override
    public void open(final Serializable checkpoint) throws Exception {
        super.init();
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        jacksonCollection.insert(items);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
