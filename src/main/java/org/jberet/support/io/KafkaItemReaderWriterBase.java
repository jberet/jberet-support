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
import java.util.Properties;
import javax.batch.api.BatchProperty;
import javax.inject.Inject;

/**
 * Base class for {@link KafkaItemReader} and {@link KafkaItemWriter}.
 *
 * @since 1.3.0
 */
public abstract class KafkaItemReaderWriterBase {
    /**
     * Delimiter to separate topic name and partition number in a concatenated string.
     * For example, {@code ordersTopic:5}.
     */
    final static char topicPartitionDelimiter = ':';

    /**
     * The file path or URL to the Kafka configuration properties file. See Kafka docs for valid keys and values.
     *
     * @see "org.apache.kafka.clients.consumer.ConsumerConfig"
     * @see "org.apache.kafka.clients.producer.ProducerConfig"
     */
    @Inject
    @BatchProperty
    protected String configFile;

    /**
     * Loads properties from the path or URL specified in batch property {@link #configFile}, and returns the properties.
     *
     * @return {@code java.util.Properties} loaded from {@code configFile}
     * @throws IOException if error occurs
     */
    protected Properties createConfigProperties() throws IOException {
        final Properties configProps = new Properties();
        if (configFile != null) {
            configProps.load(ItemReaderWriterBase.getInputStream(configFile, false));
        }
        return configProps;
    }
}
