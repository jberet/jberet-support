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

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.batch.runtime.BatchStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JacksonCsvItemReaderWriterTest extends CsvItemReaderWriterTest {
    private static final String jobName = "org.jberet.support.io.JacksonCsvReaderTest";

    private String jsonParserFeatures;
    private String csvParserFeatures;
    private String deserializationProblemHandlers;
    private String inputDecorator;

    private String jsonGeneratorFeatures;
    private String csvGeneratorFeatures = "ALWAYS_QUOTE_STRINGS=false, STRICT_CHECK_FOR_QUOTING=true";
    private String outputDecorator;

    private String lineSeparator;
    private String escapeChar;
    private String skipFirstDataRow;
    private String nullValue;

    @Test
    public void testBeanType() throws Exception {
        //override the default quote char ", which is used in feetInches cell
        testReadWrite0(personResource, "testBeanType.out", null, null,
                Person2.class.getName(), true, Person2.class.getName(),
                null, "|",
                personResourceExpect, personResourceForbid);
    }

    @Test
    public void testBeanTypeTab() throws Exception {
        //override the default quote char ", which is used in feetInches cell
        testReadWrite0(personTabResource, "testBeanTypeTab.out", null, null,
                Person2.class.getName(), true, nameMapping,
                "\t", "|",
                null, null);
    }

    @Test
    public void testBeanTypePipe() throws Exception {
        //override the default quote char ", which is used in feetInches cell. | is already used as the delimiterChar
        //so cannot be used as quoteChar again.
        testReadWrite0(personPipeResource, "testBeanTypePipe.out", null, null,
                Person2.class.getName(), true, nameMapping,
                "|", "^",
                null, null);
    }

    /**
     * This test method and {@link #testStringArrayType()} have {@code beanType} List or String[] for raw access to
     * CSV data. So CSV schema will not be used and any schema-related configurations will not take affect.
     * header and comment line are also read as part of raw data.
     * Need to count in header and comment line when set start, end, expected strings and forbidden strings.
     *
     * @throws Exception
     *
     * @see #testStringArrayType()
     */
    @Test
    public void testListType() throws Exception {
        testReadWrite0(personResource, "testListType.out", "8", "11",
                java.util.List.class.getName(), false, nameMapping,
                null, "|",
                personResourceExpect, personResourceForbid);
    }

    /**
     * reads csv raw data and convert each line data into String[], similar to {@link #testListType()}.
     *
     * @throws Exception
     *
     * @see #testListType()
     */
    @Test
    public void testStringArrayType() throws Exception {
        testReadWrite0(personResource, "testStringArrayType.out", "8", "11",
                String[].class.getName(), false, nameMapping,
                null, "|",
                personResourceExpect, personResourceForbid);
    }

    @Test
    public void testJsonNodeType() throws Exception {
        testReadWrite0(personResource, "testJsonNodeType.out", null, null,
                JsonNode.class.getName(), true, nameMapping,
                null, "|",
                personResourceExpect, personResourceForbid);
    }

    @Test
    public void testMapType() throws Exception {
        testReadWrite0(personResource, "testMapType.out", null, null,
                java.util.Map.class.getName(), true, nameMapping,
                null, "|",
                personResourceExpect, personResourceForbid);
    }

    @Override
    public void testInvalidWriteResource() throws Exception {
    }

    @Override
    public void testStringsToInts() throws Exception {
    }

    //test will print out the path of output file from CsvItemWriter, which can then be verified.
    //e.g., CSV resource to read:
    //fake-person.csv,
    //to write:
    //        /var/folders/s3/2m3bc7_n0550tp44h4bcgwtm0000gn/T/testMapType.out
    private void testReadWrite0(final String resource, final String writeResource, final String start, final String end,
                                final String beanType, final boolean useHeader, final String columns,
                                final String columnSeparator, final String quoteChar,
                                final String expect, final String forbid) throws Exception {
        final Properties params = createParams(CsvProperties.BEAN_TYPE_KEY, beanType);
        params.setProperty(CsvProperties.RESOURCE_KEY, resource);

        if (start != null) {
            params.setProperty("start", start);
        }
        if (end != null) {
            params.setProperty("end", end);
        }
        if (useHeader) {
            params.setProperty("useHeader", String.valueOf(useHeader));
        }
        if (columns != null) {
            params.setProperty("columns", columns);
        }
        if (columnSeparator != null) {
            params.setProperty("columnSeparator", columnSeparator);
        }
        if (quoteChar != null) {
            params.setProperty("quoteChar", quoteChar);
        }

        if (lineSeparator != null) {
            params.setProperty("lineSeparator", lineSeparator);
        }
        if (escapeChar != null) {
            params.setProperty("escapeChar", escapeChar);
        }
        if (skipFirstDataRow != null) {
            params.setProperty("skipFirstDataRow", skipFirstDataRow);
        }
        if (nullValue != null) {
            params.setProperty("nullValue", nullValue);
        }

        if (jsonParserFeatures != null) {
            params.setProperty("jsonParserFeatures", jsonParserFeatures);
        }
        if (csvParserFeatures != null) {
            params.setProperty("csvParserFeatures", csvParserFeatures);
        }
        if (deserializationProblemHandlers != null) {
            params.setProperty("deserializationProblemHandlers", deserializationProblemHandlers);
        }
        if (inputDecorator != null) {
            params.setProperty("inputDecorator", inputDecorator);
        }

        if (jsonGeneratorFeatures != null) {
            params.setProperty("jsonGeneratorFeatures", jsonGeneratorFeatures);
        }
        if (csvGeneratorFeatures != null) {
            params.setProperty("csvGeneratorFeatures", csvGeneratorFeatures);
        }
        if (outputDecorator != null) {
            params.setProperty("outputDecorator", outputDecorator);
        }

        final File writeResourceFile = new File(tmpdir, writeResource);
        params.setProperty("writeResource", writeResourceFile.getPath());

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(waitTimeoutMinutes, TimeUnit.MINUTES);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());
        validate(writeResourceFile, expect, forbid);
    }
}
