/*
 * Copyright (c) 2015 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import jakarta.batch.api.chunk.AbstractItemWriter;
import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CsvReadersPerformanceTest {
    private static final String superCsvjobName = "org.jberet.support.io.superCsvTest";
    private static final String jacksonCsvjobName = "org.jberet.support.io.jacksonCsvTest";

    static final String ibmStockTradeColumns = "Date, Time, Open, High, Low, Close, Volume";
    private final JobOperator jobOperator = BatchRuntime.getJobOperator();

    @Test
    public void superCsvBeanTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", StockTrade.class.getName());
        params.setProperty("nameMapping", ExcelWriterTest.ibmStockTradeNameMapping);
        testReadWrite0(superCsvjobName, params);
    }

    @Test
    public void superCsvMapTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", java.util.Map.class.getName());
        params.setProperty("nameMapping", ExcelWriterTest.ibmStockTradeNameMapping);
        testReadWrite0(superCsvjobName, params);
    }

    @Test
    public void superCsvListTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", java.util.List.class.getName());
        //params.setProperty("nameMapping", ExcelWriterTest.ibmStockTradeNameMapping);
        testReadWrite0(superCsvjobName, params);
    }

    ///////////////////////////////////////////////////////

    @Test
    public void jacksonCsvBeanTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", StockTrade2.class.getName());
        params.setProperty("columns", StockTrade2.class.getName());  //use POJO to define CSV schema
        testReadWrite0(jacksonCsvjobName, params);
    }

    @Test
    public void jacksonCsvBeanTypeFull2() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", StockTrade2.class.getName());
        params.setProperty("columns", ibmStockTradeColumns);  //manually build CSV schema
        testReadWrite0(jacksonCsvjobName, params);
    }

    @Test
    public void jacksonCsvMapTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", java.util.Map.class.getName());
        params.setProperty("columns", StockTrade.class.getName());  //use POJO to define CSV schema
        testReadWrite0(jacksonCsvjobName, params);
    }

    @Test
    public void jacksonCsvListTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", java.util.List.class.getName());
        //params.setProperty("columns", StockTrade.class.getName());
        testReadWrite0(jacksonCsvjobName, params);
    }

    @Test
    public void jacksonCsvStringArrayTypeFull() throws Exception {
        final Properties params = new Properties();
        params.setProperty("beanType", String[].class.getName());
        //params.setProperty("columns", StockTrade.class.getName());
        testReadWrite0(jacksonCsvjobName, params);
    }

    ///////////////////////////////////////////////////////

    private void testReadWrite0(final String jobName, final Properties params) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.MINUTES);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());
        final long duration = System.currentTimeMillis() - startTime;
        System.out.printf("%s\t\t%s seconds%n", jobName, duration / 1000.0);
    }


    @Named
    @Dependent
    public static final class NoopItemWriter extends AbstractItemWriter {
        @Override
        public void writeItems(final List<Object> items) throws Exception {
            System.out.printf("%s items%n", items.size());
            final Object firstItem = items.get(0);
            System.out.printf("%s%n", firstItem instanceof String[] ? Arrays.asList((String[]) firstItem) : firstItem);
        }
    }
}
