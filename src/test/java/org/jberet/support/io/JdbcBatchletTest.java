/*
 * Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import static org.jberet.support.io.JdbcReaderWriterTest.getConnection;
import static org.jberet.support.io.JdbcReaderWriterTest.jobOperator;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import jakarta.batch.runtime.BatchStatus;
import jakarta.batch.runtime.StepExecution;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcBatchletTest {
    static final String jdbcBatchletJobName = "org.jberet.support.io.JdbcBatchletTest";
    static final String insertSql =
            "insert into STOCK_TRADE (TRADEDATE, TRADETIME, OPEN, HIGH, LOW, CLOSE, VOLUMN) VALUES" +
                    "('2017-02-17','15:51',122.25,122.44,122.25,122.44,16800.0)";

    static final String sqls = insertSql + ";" + JdbcReaderWriterTest.deleteAllRows;

    @BeforeAll
    public static void beforeClass() throws Exception {
        JdbcReaderWriterTest.initTable();
    }

    @Test
    public void multipleSqls() throws Exception {
        runTest(sqls, BatchStatus.COMPLETED);
    }

    @Test
    public void singleSql() throws Exception {
        runTest(JdbcReaderWriterTest.readerQuery, BatchStatus.COMPLETED);
    }

    @Test
    public void multipleSqlsInvalid() throws Exception {
        runTest(sqls + ";" + "xxx", BatchStatus.FAILED);
    }

    @Test
    public void storedProcedure() throws Exception {
        final String storedProcedureDef = "CREATE ALIAS IF NOT EXISTS sp2 AS $$" +
                "void sp2(Connection conn) throws SQLException {" +
                "    conn.createStatement().executeUpdate(\"delete from STOCK_TRADE\");" +
                "}$$;";
        final String callStoredProcedure = "{ call sp2() }";

        final Connection connection = getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(storedProcedureDef);
            System.out.printf("Created stored procedure sp2 as %s%n", storedProcedureDef);
        } finally {
            JdbcItemReaderWriterBase.close(connection, statement);
        }

        runTest(callStoredProcedure, BatchStatus.COMPLETED);
    }

    private void runTest(final String sqls, final BatchStatus batchStatus) throws Exception {
        final Properties jobParams = new Properties();
        jobParams.setProperty("sqls", sqls);
        jobParams.setProperty("url", JdbcReaderWriterTest.url);
        final long jobExecutionId = jobOperator.start(jdbcBatchletJobName, jobParams);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(5, TimeUnit.MINUTES);
        assertEquals(batchStatus, jobExecution.getBatchStatus());
        assertEquals(batchStatus.toString(), jobExecution.getExitStatus());

        if (batchStatus == BatchStatus.FAILED) {
            return;
        }

        final List<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        System.out.printf("Step exit status (sqls execution result): %s%n", stepExecutions.get(0).getExitStatus());
    }
}
