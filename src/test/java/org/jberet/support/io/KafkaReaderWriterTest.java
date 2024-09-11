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


import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests to verify {@link KafkaItemReader} and {@link KafkaItemWriter}.
 * Before running tests, need to start Zookeeper and Kafka server, in their own terminals:
 * <p>
 * cd $KAFKA_HOME
 * bin/zookeeper-server-start.sh config/zookeeper.properties
 * bin/kafka-server-start.sh config/server.properties
 * <p>
 * For debugging purpose, sometimes you may want to manually view messages in a topic:
 * bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic t1 --from-beginning
 */
@Disabled("Need to start Zookeeper and Kafka server before running these tests.")
public class KafkaReaderWriterTest {
    private static final JobOperator jobOperator = BatchRuntime.getJobOperator();
    static final String writerTestJobName = "org.jberet.support.io.KafkaWriterTest.xml";
    static final String readerTestJobName = "org.jberet.support.io.KafkaReaderTest.xml";

    static final String ibmStockTradeExpected1_50 = "09:30, 67040, 09:31, 10810,    09:39, 2500, 10:18, 10:19";
    static final String ibmStockTradeForbid1_50 = "10:20, 10:21, 10:22";

    static final String ibmStockTradeExpected1_20 = "09:30, 67040, 09:31,    09:49, 09:48, 09:47";
    static final String ibmStockTradeForbid1_20 = "09:50, 09:51, 09:52";

    static final String ibmStockTradeExpected21_50 = ibmStockTradeForbid1_20 + ", 10:19, 10:18, 10:17";
    static final String ibmStockTradeForbid21_50 = ibmStockTradeExpected1_20 + ", " + ibmStockTradeForbid1_50;

    static final String producerRecordKey = null;
    static final String pollTimeout = String.valueOf(1000);

    /**
     * First runs job {@link #writerTestJobName} that reads data from CSV file and sends to Kafka server with
     * {@link KafkaItemWriter}.
     * Then runs job {@link #readerTestJobName} that reads messages from Kafka server with
     * {@link KafkaItemReader} and writes to output file with {@link CsvItemWriter}.
     *
     * @throws Exception
     */
    @Test
    public void readIBMStockTradeCsvWriteKafkaBeanType() throws Exception {
        String topicPartition = "readIBMStockTradeCsvWriteKafkaBeanType" + System.currentTimeMillis() + ":0";
        testWrite0(writerTestJobName, StockTrade.class,
                ExcelWriterTest.ibmStockTradeHeader, ExcelWriterTest.ibmStockTradeCellProcessors,
                "1", "50", topicPartition, producerRecordKey);

        // CsvItemReaderWriter uses header "Date, Time, Open, ..."
        // CsvItemReaderWriter has nameMapping "date, time, open, ..." to match java fields in StockTrade. CsvItemReaderWriter
        // does not understand Jackson mapping annotations in POJO.

        testRead0(readerTestJobName, StockTrade.class, "readIBMStockTradeCsvWriteJmsBeanType.out",
                ExcelWriterTest.ibmStockTradeNameMapping, ExcelWriterTest.ibmStockTradeHeader,
                topicPartition, pollTimeout, null,
                ibmStockTradeExpected1_50, ibmStockTradeForbid1_50, BatchStatus.COMPLETED);
    }

    /**
     * Tests {@link KafkaItemReader} checkpoint and offset management, and restart behavior.
     * The test first reads data from CSV with {@link CsvItemReader}, and writes to Kafka server with {@link KafkaItemWriter}.
     * Then, the test reads records from Kafka server with {@link KafkaItemReader}, and writes to CSV. This step is
     * configured to fail inside the processor.
     * Finally, the test restarts the previous failed job execution, and verifies that all remaining records left over
     * from previous failed job execution are read, processed and written, and any records that were already successfully
     * processed should not appear in this restart job execution.
     *
     * @throws Exception
     */
    @Test
    public void readIBMStockTradeCsvWriteKafkaRestart() throws Exception {
        String topicPartition = "readIBMStockTradeCsvWriteKafkaRestart" + System.currentTimeMillis() + ":0";
        testWrite0(writerTestJobName, StockTrade.class,
                ExcelWriterTest.ibmStockTradeHeader, ExcelWriterTest.ibmStockTradeCellProcessors,
                "1", "50", topicPartition, producerRecordKey);

        final String writeResource = "readIBMStockTradeCsvWriteKafkaRestart.out";
        final String failOnTimes = "09:52";
        final long executionId =
                testRead0(readerTestJobName, StockTrade.class, writeResource,
                ExcelWriterTest.ibmStockTradeNameMapping, ExcelWriterTest.ibmStockTradeHeader,
                topicPartition, pollTimeout, failOnTimes,
                ibmStockTradeExpected1_20, ibmStockTradeForbid1_20, BatchStatus.FAILED);

         final Properties restartJobParams = new Properties();
         restartJobParams.setProperty("failOnTimes", "-1");
         final long restartId = jobOperator.restart(executionId, restartJobParams);
         final JobExecutionImpl restartExecutioin = (JobExecutionImpl) jobOperator.getJobExecution(restartId);
         restartExecutioin.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.HOURS);
         assertEquals(BatchStatus.COMPLETED, restartExecutioin.getBatchStatus());

         CsvItemReaderWriterTest.validate(getWriteResourceFile(writeResource),
                 ibmStockTradeExpected21_50, ibmStockTradeForbid21_50);
    }


    static void testWrite0(final String jobName, final Class<?> beanType, final String csvNameMapping, final String cellProcessors,
                    final String start, final String end,
                    final String topicPartition, final String recordKey) throws Exception {
        final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());

        if (csvNameMapping != null) {
            params.setProperty("nameMapping", csvNameMapping);
        }
        if (cellProcessors != null) {
            params.setProperty("cellProcessors", cellProcessors);
        }
        if (start != null) {
            params.setProperty("start", start);
        }
        if (end != null) {
            params.setProperty("end", end);
        }
        if (topicPartition != null) {
            params.setProperty("topicPartition", topicPartition);
        }
        if (recordKey != null) {
            params.setProperty("recordKey", recordKey);
        }

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.HOURS);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());
    }

    static long testRead0(final String jobName, final Class<?> beanType, final String writeResource,
                   final String csvNameMapping, final String csvHeader,
                   final String topicPartitions, final String pollTimeout, final String failOnTimes,
                   final String expect, final String forbid, final BatchStatus expectedStatus) throws Exception {
        final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());

        final File writeResourceFile;
        if (writeResource != null) {
            writeResourceFile = getWriteResourceFile(writeResource);
            params.setProperty("writeResource", writeResourceFile.getPath());
        } else {
            throw new RuntimeException("writeResource is null");
        }
        if (csvNameMapping != null) {
            params.setProperty("nameMapping", csvNameMapping);
        }
        if (csvHeader != null) {
            params.setProperty("header", csvHeader);
        }
        if (topicPartitions != null) {
            params.setProperty("topicPartitions", topicPartitions);
        }
        if (pollTimeout != null) {
            params.setProperty("pollTimeout", pollTimeout);
        }
        if (failOnTimes != null) {
            params.setProperty("failOnTimes", failOnTimes);
        }

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.HOURS);
        assertEquals(expectedStatus, jobExecution.getBatchStatus());
        CsvItemReaderWriterTest.validate(writeResourceFile, expect, forbid);

        return jobExecutionId;
    }

    private static File getWriteResourceFile(final String writeResource) {
        return new File(CsvItemReaderWriterTest.tmpdir, writeResource);
    }
}
