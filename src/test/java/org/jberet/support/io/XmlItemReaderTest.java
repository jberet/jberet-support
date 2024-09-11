/*
 * Copyright (c) 2014-2015 Red Hat, Inc. and/or its affiliates.
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
 * A test class that reads xml resource into java object and write out to xml format.
 */
public final class XmlItemReaderTest {
    static final String jobName = "org.jberet.support.io.XmlItemReaderTest";
    private final JobOperator jobOperator = BatchRuntime.getJobOperator();

    //the online resource may change any time, so use a local copy
    //static final String movieXml = "http://mysafeinfo.com/api/data?list=topmoviesboxoffice2012&format=xml";
    static final String movieXml = "movies-2012.xml";

    // openstreammap file, 265M in size, make sure XmlItemReader and XmlItemWriter can handle large file, with
    // xml attributes, and sub-elements that are serialized unwrapped.
    // the osm was downloaded from http://osm.kewl.lu/luxembourg.osm/
    // jberet-support can read resource from a url, but given the size of this file, it will take about 30 minutes to
    // just download the file.  So it's best to download this file beforehand and have the test access the local copy.
    static final String osmXml = "/Users/cfang/tmp/luxembourg-20140218_173810.osm";

    static final String movieRootElementName = "movies";
    static final String osmRootElementName = "osm";
    private String customDataTypeModules;

    @Test
    public void testXmlMovieBeanType1_2() throws Exception {
        testReadWrite0(movieXml, "testXmlMovieBeanType1_2.out", "1", "2", Movie.class, MovieTest.expect1_2, MovieTest.forbid1_2);
    }

    @Test
    public void testXmlMovieBeanType2_4() throws Exception {
        testReadWrite0(movieXml, "testXmlMovieBeanType2_4.out", "2", "4", Movie.class, MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testXmlMovieBeanTypeFull() throws Exception {
        testReadWrite0(movieXml, "testXmlMovieBeanTypeFull.out", null, null, Movie.class, null, null);
    }

    @Test
    public void testXmlMovieBeanTypeJodaFull() throws Exception {
        customDataTypeModules = "com.fasterxml.jackson.datatype.joda.JodaModule";
        testReadWrite0(movieXml, "testXmlMovieBeanTypeFull.out", null, null, MovieWithJoda.class, null, null);
        customDataTypeModules = null;
    }

    @Test
    public void testXmlMovieBeanTypeFull1_100() throws Exception {
        testReadWrite0(movieXml, "testXmlMovieBeanTypeFull1_100.out", "1", "100", Movie.class, MovieTest.expectFull, null);
    }

    @Test
    @Disabled
    //takes about 20 seconds
    public void testXmlOsmBeanTypeFull() throws Exception {
        final String writeResource = "testXmlOsmBeanTypeFull.out";
        final File file = new File(CsvItemReaderWriterTest.tmpdir, writeResource);
        file.delete();
        testReadWrite0(osmXml, writeResource, null, null, OsmNode.class, null, null);
    }

    private void testReadWrite0(final String resource, final String writeResource,
                                final String start, final String end, final Class<?> beanType,
                                final String expect, final String forbid) throws Exception {
        final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());
        params.setProperty(CsvProperties.RESOURCE_KEY, resource);

        final File file = new File(CsvItemReaderWriterTest.tmpdir, writeResource);
        params.setProperty("writeResource", file.getPath());
        if (resource.equals(movieXml)) {
            params.setProperty("rootElementName", movieRootElementName);
        } else if (resource.equals(osmXml)) {
            params.setProperty("rootElementName", osmRootElementName);
        } else {
            throw new IllegalStateException("Unknown resource: " + resource);
        }

        if (start != null) {
            params.setProperty(CsvProperties.START_KEY, start);
        }
        if (end != null) {
            params.setProperty(CsvProperties.END_KEY, end);
        }
        if (customDataTypeModules != null) {
            params.setProperty("customDataTypeModules", customDataTypeModules);
        }
        CsvItemReaderWriterTest.setRandomWriteMode(params);

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes * 100, TimeUnit.MINUTES);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());

        if (!resource.equals(osmXml)) {
            //avoid reading the very large osm xml output file
            CsvItemReaderWriterTest.validate(file, expect, forbid);
        }
    }
}
