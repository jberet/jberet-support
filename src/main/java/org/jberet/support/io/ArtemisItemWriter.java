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

import java.io.Serializable;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.jberet.support._private.SupportLogger;

import jakarta.batch.api.BatchProperty;
import jakarta.batch.api.chunk.ItemWriter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

/**
 * An implementation of {@code jakarta.batch.api.chunk.ItemWriter} that sends data items to an Artemis address.
 * It can send the following Artemis message types:
 * <p>
 * <ul>
 * <li>if the data item is of type {@code java.lang.String}, a {@code org.apache.activemq.artemis.api.core.client.ClientMessage#TEXT_TYPE}
 * message is created with the text content in the data item, and sent;
 * <li>else if the data is of type {@code org.apache.activemq.artemis.api.core.client.ClientMessage}, it is sent as is;
 * <li>else an {@code org.apache.activemq.artemis.api.core.client.ClientMessage#OBJECT_TYPE} message is created with the data item
 * object, and sent.
 * </ul>
 * <p>
 * {@link #durableMessage} property can be configured to send either durable or non-durable (default) messages.
 *
 * @see     ArtemisItemReader
 * @see     ArtemisItemReaderWriterBase
 * @see     JmsItemWriter
 * @since   1.3.0
 */
@Named
@Dependent
public class ArtemisItemWriter extends ArtemisItemReaderWriterBase implements ItemWriter {
    /**
     * Whether the message to be produced is durable or not. Optional property and defaults to false. Valid values are
     * true and false.
     */
    @Inject
    @BatchProperty
    protected boolean durableMessage;

    protected ClientProducer producer;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        super.open(checkpoint);
        producer = session.createProducer(queueAddress);
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            final ClientMessage msg;
            if (item instanceof ClientMessage) {
                msg = (ClientMessage) item;
            } else if (item instanceof String) {
                msg = session.createMessage(ClientMessage.TEXT_TYPE, durableMessage);
                msg.getBodyBuffer().writeString((String) item);
            } else {
                msg = session.createMessage(ClientMessage.OBJECT_TYPE, durableMessage);
                msg.getBodyBuffer().writeBytes(objectToBytes(item));
            }
            producer.send(msg);
        }
    }

    @Override
    public void close() {
        super.close();
        if (producer != null) {
            try {
                producer.close();
            } catch (final ActiveMQException e) {
                SupportLogger.LOGGER.tracef(e, "Failed to close Artemis consumer %s%n", producer);
            }
            producer = null;
        }
    }
}
