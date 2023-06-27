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

import static org.jberet.support.io.JpaResourceProducer.em;
import static org.jberet.support.io.JpaResourceProducer.emf;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.persistence.Persistence;

import org.jberet.runtime.JobExecutionImpl;
import org.jberet.runtime.StepExecutionImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import jakarta.batch.runtime.StepExecution;

public final class JpaItemReaderWriterTest {
    private static final JobOperator jobOperator = BatchRuntime.getJobOperator();
    private static final String jpaItemWriterJob = "org.jberet.support.io.jpaItemWriterTest";
    private static final String jpaItemReaderJob = "org.jberet.support.io.jpaItemReaderTest";
    static final String persistenceUnitName = "JpaItemWriterTest";

    @BeforeClass
    public static void beforeClass() {
        emf = Persistence.createEntityManagerFactory(persistenceUnitName);
        em = emf.createEntityManager();
    }

    @AfterClass
    public static void afterClass() {
        if (em != null) {
            em.close();
        }
        if (emf != null) {
            emf.close();
        }
    }

    @Test
    public void nativeQuery() throws Exception {
        final String testName = "nativeQuery";
        long jobExecutionId = jobOperator.start(jpaItemWriterJob, null);
        JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(5, TimeUnit.MINUTES);

        List<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        StepExecutionImpl step1 = (StepExecutionImpl) stepExecutions.get(0);
        System.out.printf("%s, %s, %s%n", step1.getStepName(), step1.getBatchStatus(), step1.getException());
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());

        final Properties jobParams = new Properties();
        jobParams.setProperty("resource",
                (new File(CsvItemReaderWriterTest.tmpdir, testName + ".txt")).getPath());
        jobExecutionId = jobOperator.start(jpaItemReaderJob, jobParams);
        jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(5, TimeUnit.MINUTES);

        stepExecutions = jobExecution.getStepExecutions();
        step1 = (StepExecutionImpl) stepExecutions.get(0);
        System.out.printf("%s, %s, %s%n", step1.getStepName(), step1.getBatchStatus(), step1.getException());
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());

    }

}
