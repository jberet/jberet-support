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

import static org.jberet.support.io.JmsReaderWriterTest.ibmStockTradeCellProcessorsDateAsString;
import static org.jberet.support.io.JmsReaderWriterTest.ibmStockTradeExpected1_10;
import static org.jberet.support.io.JmsReaderWriterTest.ibmStockTradeForbid1_10;
import static org.jberet.support.io.JmsReaderWriterTest.testRead0;
import static org.jberet.support.io.JmsReaderWriterTest.testWrite0;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArtemisReaderWriterTest {
    static final String writerTestJobName = "org.jberet.support.io.ArtemisWriterTest.xml";
    static final String readerTestJobName = "org.jberet.support.io.ArtemisReaderTest.xml";

    //static final String connectorFactoryName = "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";
    //static final String acceptorFactoryName = "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory";

    static final String connectorFactoryName = "org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory";
    static final String acceptorFactoryName = "org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory";

    static final String queueAddress = "example";

    ActiveMQServer server;
    ClientSession coreSession;

    @BeforeEach
    public void before() throws Exception {
        //Create the Configuration, and set the properties accordingly
        final Configuration configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactoryName));

        //Create and start the server
        server = ActiveMQServers.newActiveMQServer(configuration);
        server.start();

        final TransportConfiguration transportConfiguration = new TransportConfiguration(connectorFactoryName);
        MessagingResourceProducer.artemisServerLocator = ActiveMQClient.createServerLocatorWithoutHA(transportConfiguration);
        MessagingResourceProducer.artemisServerLocator.setBlockOnAcknowledge(false);
        MessagingResourceProducer.artemisServerLocator.setConfirmationWindowSize(5);

        MessagingResourceProducer.artemisSessionFactory = MessagingResourceProducer.artemisServerLocator.createSessionFactory();
        coreSession = MessagingResourceProducer.artemisSessionFactory.createSession(false, false, false);
        coreSession.createQueue(queueAddress, queueAddress);
    }

    @AfterEach
    public void after() throws Exception {
        if (coreSession != null) {
            coreSession.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void readIBMStockTradeCsvWriteArtemisBeanType() throws Exception {
        testWrite0(writerTestJobName, StockTrade.class, ExcelWriterTest.ibmStockTradeHeader, ExcelWriterTest.ibmStockTradeCellProcessors,
                "1", "10");

        // CsvItemReaderWriter uses header "Date, Time, Open, ..."
        // CsvItemReaderWriter has nameMapping "date, time, open, ..." to match java fields in StockTrade. CsvItemReaderWriter
        // does not understand Jackson mapping annotations in POJO.

        testRead0(readerTestJobName, StockTrade.class, "readIBMStockTradeCsvWriteArtemisBeanType.out",
                ExcelWriterTest.ibmStockTradeNameMapping, ExcelWriterTest.ibmStockTradeHeader,
                ibmStockTradeExpected1_10, ibmStockTradeForbid1_10);
    }

    @Test
    public void readIBMStockTradeCsvWriteArtemisMapType() throws Exception {
        testWrite0(writerTestJobName, Map.class, ExcelWriterTest.ibmStockTradeHeader, ibmStockTradeCellProcessorsDateAsString,
                "1", "10");

        testRead0(readerTestJobName, Map.class, "readIBMStockTradeCsvWriteArtemisMapType.out",
                ExcelWriterTest.ibmStockTradeNameMapping, ExcelWriterTest.ibmStockTradeHeader,
                ibmStockTradeExpected1_10, ibmStockTradeForbid1_10);
    }

    @Test
    public void readIBMStockTradeCsvWriteArtemisListType() throws Exception {
        testWrite0(writerTestJobName, List.class, ExcelWriterTest.ibmStockTradeHeader, ExcelWriterTest.ibmStockTradeCellProcessors,
                "1", "10");

        testRead0(readerTestJobName, List.class, "readIBMStockTradeCsvWriteArtemisListType.out",
                ExcelWriterTest.ibmStockTradeNameMapping, ExcelWriterTest.ibmStockTradeHeader,
                ibmStockTradeExpected1_10, ibmStockTradeForbid1_10);
    }

    public static class ArtemisSendAcknowledgementHandler implements SendAcknowledgementHandler {
        @Override
        public void sendAcknowledged(final Message message) {
            System.out.printf("sendAcknowledged message: %s in %s%n", message, this);
        }
    }
}
