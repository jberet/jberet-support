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
import java.util.Map;

import org.jberet.support._private.SupportLogger;

import jakarta.batch.api.chunk.ItemWriter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;

/**
 * An implementation of {@code jakarta.batch.api.chunk.ItemWriter} that sends data items to a JMS destination. It can
 * sends the following JMS message types:
 * <p>
 * <ul>
 * <li>if the data item is of type {@code java.util.Map}, a {@code MapMessage} is created, populated with the data
 * contained in the data item, and sent;
 * <li>else if the data item is of type {@code java.lang.String}, a {@code TextMessage} is created with the text content
 * in the data item, and sent;
 * <li>else if the data is of type {@code jakarta.jms.Message}, it is sent as is;
 * <li>else an {@code ObjectMessage} is created with the data item object, and sent.
 * </ul>
 * <p>
 *
 * @see     JmsItemReader
 * @see     JmsItemReaderWriterBase
 * @since   1.1.0
 */
@Named
@Dependent
public class JmsItemWriter extends JmsItemReaderWriterBase implements ItemWriter {
    protected MessageProducer producer;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        super.open(checkpoint);
        producer = session.createProducer(destination);
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            final Message msg;
            if (item instanceof Map) {
                final Map<?, ?> itemAsMap = (Map) item;
                final MapMessage mapMessage = session.createMapMessage();
                for (final Map.Entry e : itemAsMap.entrySet()) {
                    mapMessage.setObject(e.getKey().toString(), e.getValue());
                }
                msg = mapMessage;
            } else if (item instanceof String) {
                msg = session.createTextMessage((String) item);
            } else if (item instanceof Message) {
                msg = (Message) item;
            } else {
                msg = session.createObjectMessage((Serializable) item);
            }
            producer.send(msg);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }

    @Override
    public void close() {
        super.close();
        if (producer != null) {
            try {
                producer.close();
            } catch (final JMSException e) {
                SupportLogger.LOGGER.tracef(e, "Failed to close JMS consumer %s%n", producer);
            }
            producer = null;
        }
    }
}
