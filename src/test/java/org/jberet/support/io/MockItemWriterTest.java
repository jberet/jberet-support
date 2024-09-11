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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test class that writes batch data to {@link MockItemWriter}.
 */
public final class MockItemWriterTest {
    static final String jobName = "org.jberet.support.io.MockItemWriterTest";
    private static final JobOperator jobOperator = BatchRuntime.getJobOperator();

    @Test
    public void toConsoleDefault() throws Exception {
        verifyJobExecution(jobOperator.start(jobName, null), BatchStatus.COMPLETED);
    }

    @Test
    public void toConsole() throws Exception {
        final Properties jobParams = new Properties();
        jobParams.setProperty("toConsole", Boolean.TRUE.toString());
        verifyJobExecution(jobOperator.start(jobName, jobParams), BatchStatus.COMPLETED);
    }

    @Test
    public void noop() throws Exception {
        final Properties jobParams = new Properties();
        jobParams.setProperty("toConsole", Boolean.FALSE.toString());
        verifyJobExecution(jobOperator.start(jobName, jobParams), BatchStatus.COMPLETED);
    }

    @Test
    public void toFile() throws Exception {
        final Properties jobParams = new Properties();
        jobParams.setProperty("toFile",
            new File(CsvItemReaderWriterTest.tmpdir, "MockItemWriterTest-toFile.txt").getAbsolutePath());
        verifyJobExecution(jobOperator.start(jobName, jobParams), BatchStatus.COMPLETED);
    }

    @Test
    public void toClass() throws Exception {
        if (DataHolder.data != null) {
            DataHolder.data.clear();
        }
        final Properties jobParams = new Properties();
        jobParams.setProperty("toClass", DataHolder.class.getName());

        verifyJobExecution(jobOperator.start(jobName, jobParams), BatchStatus.COMPLETED);
        assertEquals(true, DataHolder.data.size() > 0);

        System.out.printf("data list size : %s, first item: %s%n",
                DataHolder.data.size(), DataHolder.data.get(0));

    }

    @Test
    public void toClassUninitialized() throws Exception {
        if (DataHolderUninitialized.data != null) {
            DataHolderUninitialized.data.clear();
        }
        final Properties jobParams = new Properties();
        jobParams.setProperty("toClass", DataHolderUninitialized.class.getName());

        verifyJobExecution(jobOperator.start(jobName, jobParams), BatchStatus.COMPLETED);
        assertEquals(true, DataHolderUninitialized.data.size() > 0);

        System.out.printf("data list size : %s, first item: %s%n",
                DataHolderUninitialized.data.size(), DataHolderUninitialized.data.get(0));

    }

    public static void verifyJobExecution(final long jobExecutionId, final BatchStatus expected)
            throws Exception {
        final JobExecutionImpl jobExecution =
                (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(5, TimeUnit.MINUTES);
        assertEquals(expected, jobExecution.getBatchStatus());
    }


    public static final class DataHolder {
        public static final List data = new ArrayList();
    }

    public static final class DataHolderUninitialized {
        public static volatile List data;
    }
}
