/*
 * Copyright (c) 2014-2018 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.Queue;

@Named
@ApplicationScoped
public class MessagingResourceProducer {
    // JMS resources
    static ConnectionFactory connectionFactory;
    static Queue queue;

    // Artemis resources
    static org.apache.activemq.artemis.api.core.client.ServerLocator artemisServerLocator;
    static org.apache.activemq.artemis.api.core.client.ClientSessionFactory artemisSessionFactory;


    @Produces
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Produces
    public Destination getDestination() {
        return queue;
    }

    @Produces
    public org.apache.activemq.artemis.api.core.client.ServerLocator getArtemisServerLocator() {
        return artemisServerLocator;
    }

    @Produces
    public org.apache.activemq.artemis.api.core.client.ClientSessionFactory getArtemisSessionFactory() {
        return artemisSessionFactory;
    }
}
