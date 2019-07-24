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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.List;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.beanio.BeanWriter;
import org.beanio.StreamFactory;

/**
 * An implementation of {@code javax.batch.api.chunk.ItemWriter} based on BeanIO. This writer class handles all
 * data formats that are supported by BeanIO, e.g., fixed length file, CSV file, XML, etc. It also supports
 * dynamic BeanIO mapping properties, which are specified in job xml, injected into this class, and can be referenced
 * in BeanIO mapping file. {@link org.jberet.support.io.BeanIOItemWriter} configurations are specified as
 * reader properties in job xml, and BeanIO mapping xml file.
 *
 * @see     BeanIOItemReaderWriterBase
 * @see     BeanIOItemReader
 * @since   1.1.0
 */
@Named
@Dependent
public class BeanIOItemWriter extends BeanIOItemReaderWriterBase implements ItemWriter {
    private BeanWriter beanWriter;

    /**
     * Instructs {@link BeanIOItemWriter}, when the target resource already
     * exists, whether to append to, or overwrite the existing resource, or fail. Valid values are {@code append},
     * {@code overwrite}, and {@code failIfExists}. Optional property, and defaults to {@code overwrite}.
     */
    @Inject
    @BatchProperty
    protected String writeMode;
    
    @Override
    public void open(final Serializable checkpoint) throws Exception {
        mappingFileKey = new StreamFactoryKey(jobContext, streamMapping);
        final StreamFactory streamFactory = getStreamFactory(streamFactoryLookup, mappingFileKey, mappingProperties);
        final OutputStream outputStream = getOutputStream(writeMode==null ? CsvProperties.OVERWRITE : writeMode);
        final Writer outputWriter = charset == null ? new OutputStreamWriter(outputStream) :
                new OutputStreamWriter(outputStream, charset);
        beanWriter = streamFactory.createWriter(streamName, new BufferedWriter(outputWriter));
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object e : items) {
            beanWriter.write(e);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }

    @Override
    public void close() throws Exception {
        if (beanWriter != null) {
            beanWriter.close();
            beanWriter = null;
            mappingFileKey = null;
        }
    }
}
