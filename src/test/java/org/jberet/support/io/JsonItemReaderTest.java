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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;
import com.fasterxml.jackson.core.io.OutputDecorator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test class that reads json resource into java object and write out to json format.
 */
public final class JsonItemReaderTest {
    static final String jobName =  "org.jberet.support.io.JsonItemReaderTest";
    private final JobOperator jobOperator = BatchRuntime.getJobOperator();
    static final String movieJson = "movies-2012.json";
    static final String movieJsonUnknownProperty = "movies-2012-unknown-properties.json";
    static final String widgetJson = "widget.json";
    static final String githubJson = "github-repos-data.json";

    static final String widgetExpect1 = "\"widget\", \"debug\", \"window\", \"title\", " +
            "\"Sample Konfabulator Widget\", \"name\", \"main_window\", \"width\"," +
            "500, \"height\", \"image\", \"src\", \"Images/Sun.png\", \"sun1\", \"hOffset\", 250," +
            "\"vOffset\", \"alignment\", \"center\", \"text\", \"data\", \"Click Here\", \"size\"," +
            "36, \"style\", \"bold\", \"text1\", \"onMouseUp\", " +
            "\"sun1.opacity = (sun1.opacity / 100) * 90;\"";
    static final String widgetExpect2 = "\"Two Widget\", \"two_window\", \"Images/Two.png\"";
    static final String widgetExpect3 = "\"Three Widget\", \"three_window\", \"Images/Three.png\"";
    static final String widgetForbidFrom1 = "Sample Konfabulator Widget, main_window, Images/Sun.png";

    private String jsonGeneratorFeatures;
    private String deserializationProblemHandlers;
    private String customDataTypeModules;

    //https://issues.jboss.org/browse/JBERET-44
    //no transaction started after a chunk where no data is written.
    //the ItemProcessor in this test is configured to filter out all items so there is always no data to write out.
    //when a new chunk is started, TransactionManager should also begin a new tx.
    @Test
    public void testMovieFilterProcessor() throws Exception {
        final String filtering = "filtering";
        System.setProperty(filtering, "true");
        testReadWrite0(movieJson, "testMovieFilterProcessor.out", "1", "15", Movie.class, null, null, BatchStatus.COMPLETED);
        System.clearProperty(filtering);
    }

    @Test
    public void testBeanTypeGithubJson1_999() throws Exception {
        testReadWrite0(githubJson, "testBeanTypeGithubJson1_999.out", "1", "999", GithubData.class, null, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testBeanTypeGithubJson1() throws Exception {
        testReadWrite0(githubJson, "testBeanTypeGithubJson1.out", "1", "1", GithubData.class, null, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testBeanTypeGithubJson2_3() throws Exception {
        testReadWrite0(githubJson, "testBeanTypeGithubJson2_3.out", "2", "3", GithubData.class, null, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testNodeTypeGithubJson() throws Exception {
        testReadWrite0(githubJson, "testNodeTypeGithubJson.out", null, null, JsonNode.class, null, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testBeanType2_4() throws Exception {
        jsonGeneratorFeatures = " QUOTE_FIELD_NAMES = false , STRICT_DUPLICATE_DETECTION = false";

        //rating enum is written out as index, as configured in job xml serializationFeatures property
        //final String forbid = MovieTest.forbid2_4 + ", \"rank\", \"tit\", \"grs\", \"opn\", PG13";
        final String forbid = MovieTest.forbid2_4 + ", PG13";
        testReadWrite0(movieJson, "testBeanType2_4.out", "2", "4", Movie.class, MovieTest.expect2_4, forbid, BatchStatus.COMPLETED);
        jsonGeneratorFeatures = null;
    }

    @Test
    public void testJsonNodeType2_4() throws Exception {
        testReadWrite0(movieJson, "testJsonNodeType2_4.out", "2", "4", JsonNode.class, MovieTest.expect2_4, MovieTest.forbid2_4, BatchStatus.COMPLETED);
    }

    @Test
    public void testBeanTypeFull() throws Exception {
        testReadWrite0(movieJson, "testBeanTypeFull.out", null, null, Movie.class, MovieTest.expectFull, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testBeanTypeJodaFull() throws Exception {
        customDataTypeModules = "com.fasterxml.jackson.datatype.joda.JodaModule";
        testReadWrite0(movieJson, "testBeanTypeJodaFull.out", null, null, MovieWithJoda.class, MovieTest.expectFull, null, BatchStatus.COMPLETED);
        customDataTypeModules = null;
    }

    @Test
    public void testMapTypeFull1_100() throws Exception {
        testReadWrite0(movieJson, "testMapTypeFull1_100.out", "1", "100", Map.class, MovieTest.expectFull, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testJsonMapType1_2() throws Exception {
        testReadWrite0(movieJson, "testJsonMapType1_2.out", "1", "2", Map.class, MovieTest.expect1_2, MovieTest.forbid1_2, BatchStatus.COMPLETED);
    }

    @Test
    public void testJsonNodeTypeWidget1() throws Exception {
        testReadWrite0(widgetJson, "testJsonNodeTypeWidget1.out", "1", "1", JsonNode.class, widgetExpect1, widgetExpect2, BatchStatus.COMPLETED);
    }

    @Test
    public void testJsonNodeTypeWidget1_3() throws Exception {
        testReadWrite0(widgetJson, "testJsonNodeTypeWidget1_3.out", "1", "3", JsonNode.class,
                widgetExpect1 + ", " + widgetExpect2 + ", " + widgetExpect3, null, BatchStatus.COMPLETED);

        testReadWrite0(widgetJson, "testJsonNodeTypeWidget1_3.out", null, null, JsonNode.class,
                widgetExpect1 + ", " + widgetExpect2 + ", " + widgetExpect3, null, BatchStatus.COMPLETED);
    }

    @Test
    public void testMapTypeWidget2_3() throws Exception {
        testReadWrite0(widgetJson, "testMapTypeWidget2_3.out", "2", "3", Map.class,
                widgetExpect2 + ", " + widgetExpect3, widgetForbidFrom1, BatchStatus.COMPLETED);
    }

    //2 unknown properties in json file will cause the read to fail
    @Test
    public void testUnknownJsonPropertyFail1_2() throws Exception {
        testReadWrite0(movieJsonUnknownProperty, "testUnknownJsonPropertyFail1_2.out","1", "2", Movie.class,
                MovieTest.expect1_2, MovieTest.forbid1_2, BatchStatus.FAILED);
    }

    //configured 1 problem handler for unknow property 1, not for unknown property 2, so will still fail
    @Test
    public void testUnknownJsonPropertyFail2_4() throws Exception {
        this.deserializationProblemHandlers = "org.jberet.support.io.JsonItemReaderTest$UnknownHandler";
        testReadWrite0(movieJsonUnknownProperty, "testUnknownJsonPropertyFail2_4.out", "2", "4", Movie.class,
                MovieTest.expect2_4, MovieTest.forbid2_4, BatchStatus.FAILED);
        this.deserializationProblemHandlers = null;
    }

    //configured 2 problem handlers for both unknown properties, so the unknow properties are properly handled and the
    //job should complete.
    @Test
    public void testUnknownJsonProperty2_4() throws Exception {
        this.deserializationProblemHandlers =
                "org.jberet.support.io.JsonItemReaderTest$UnknownHandler, org.jberet.support.io.JsonItemReaderTest$Unknown2Handler";
        testReadWrite0(movieJsonUnknownProperty, "testUnknownJsonProperty2_4.out", "2", "4", Movie.class,
                MovieTest.expect2_4, MovieTest.forbid2_4, BatchStatus.COMPLETED);
        this.deserializationProblemHandlers=null;
    }

    private void testReadWrite0(final String resource, final String writeResource,
                                final String start, final String end, final Class<?> beanType,
                                final String expect, final String forbid, final BatchStatus jobStatus) throws Exception {
        final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());
        params.setProperty(CsvProperties.RESOURCE_KEY, resource);

        final File writeResourceFile = new File(CsvItemReaderWriterTest.tmpdir, writeResource);
        params.setProperty("writeResource", writeResourceFile.getPath());

        if (start != null) {
            params.setProperty(CsvProperties.START_KEY, start);
        }
        if (end != null) {
            params.setProperty(CsvProperties.END_KEY, end);
        }
        if (jsonGeneratorFeatures != null) {
            params.setProperty("jsonGeneratorFeatures", jsonGeneratorFeatures);
        }
        if (deserializationProblemHandlers != null) {
            params.setProperty("deserializationProblemHandlers", deserializationProblemHandlers);
        }
        if (customDataTypeModules != null) {
            params.setProperty("customDataTypeModules", customDataTypeModules);
        }

        params.setProperty(CsvProperties.HEADER_KEY, MovieTest.header);
        CsvItemReaderWriterTest.setRandomWriteMode(params);

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.MINUTES);
        assertEquals(jobStatus, jobExecution.getBatchStatus());

        if (jobStatus == BatchStatus.COMPLETED) {
            CsvItemReaderWriterTest.validate(writeResourceFile, expect, forbid);
        }
    }

    public static final class NoopInputDecorator extends InputDecorator {
        @Override
        public InputStream decorate(final IOContext ctxt, final InputStream in) throws IOException {
            System.out.printf("In decorate method of %s%n", this);
            return in;
        }

        @Override
        public InputStream decorate(final IOContext ctxt, final byte[] src, final int offset, final int length) throws IOException {
            System.out.printf("In decorate method of %s%n", this);
            return new ByteArrayInputStream(src, offset, length);
        }

        @Override
        public Reader decorate(final IOContext ctxt, final Reader src) throws IOException {
            System.out.printf("In decorate method of %s%n", this);
            return src;
        }
    }

    public static final class NoopOutputDecorator extends OutputDecorator {
        @Override
        public OutputStream decorate(final IOContext ctxt, final OutputStream out) throws IOException {
            System.out.printf("In decorate method of %s%n", this);
            return out;
        }

        @Override
        public Writer decorate(final IOContext ctxt, final Writer w) throws IOException {
            System.out.printf("In decorate method of %s%n", this);
            return w;
        }
    }

    public static final class JsonSerializer<Exception> extends com.fasterxml.jackson.databind.JsonSerializer<Exception> {
        @Override
        public void serialize(final Exception value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeObject(value);
        }
    }

    public static final class JsonDeserializer<Exception> extends com.fasterxml.jackson.databind.JsonDeserializer<Exception> {
        @Override
        public Exception deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return jp.readValueAs(new TypeReference<Exception>() {
                @Override
                public Type getType() {
                    return super.getType();
                }

                @Override
                public int compareTo(final TypeReference<Exception> o) {
                    return super.compareTo(o);
                }
            });
        }
    }

    public static final class UnknownHandler extends DeserializationProblemHandler {
        @Override
        public boolean handleUnknownProperty(final DeserializationContext ctxt,
                                             final JsonParser jp,
                                             final com.fasterxml.jackson.databind.JsonDeserializer<?> deserializer,
                                             final Object beanOrClass,
                                             final String propertyName) throws IOException, JsonProcessingException {
            if (propertyName.equals("unknown")) {
                jp.skipChildren();
                return true;
            }
            return false;
        }
    }

    public static final class Unknown2Handler extends DeserializationProblemHandler {
        @Override
        public boolean handleUnknownProperty(final DeserializationContext ctxt,
                                             final JsonParser jp,
                                             final com.fasterxml.jackson.databind.JsonDeserializer<?> deserializer,
                                             final Object beanOrClass,
                                             final String propertyName) throws IOException, JsonProcessingException {
            if (propertyName.equals("unknown2")) {
                jp.skipChildren();
                return true;
            }
            return false;
        }
    }
}
