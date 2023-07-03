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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;
import org.junit.Assert;
import org.junit.Test;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;

public final class ExcelReaderTest {
    private final JobOperator jobOperator = BatchRuntime.getJobOperator();
    static final String excelUserModelItemReader = "excelUserModelItemReader";
    static final String excelStreamingItemReader = "excelStreamingItemReader";
    static final String excelEventItemReader = "excelEventItemReader";

    static final String excelReaderTestJobName = "org.jberet.support.io.ExcelReaderTest";
    static final String personMoviesResource = "person-movies.xlsx";
    static final String moviesSheetName = "Sheet2";
    static final String personSheetName = "Sheet1";
    static final String moviesWithBlankCellSheetName = "Movies with Blank Cells";
    static final String capeResource = "ie_data.xls";
    static final String capeSimpleResource = "ie_data_simple.xls";
    static final String capeSheetName = "Data";
    static final String capeHeader =
            "date, sp, dividend, earnings, cpi, dateFraction, longInterestRate, realPrice, realDividend, realEarnings, cape";
    static final String capeSimpleHeader = capeHeader + ", flag";    //added a boolean field 'flag'
    static final String capeFullExpected = "1871.01, 1871.02, 1871.03, 1950.01, 1950.02, 1950.03, 2014.01, 2014.02, 2014.03, 2014.04";
    static final String capeExpected20_25 = "1872.01, 1872.02, 1872.03, 1872.04, 1872.05, 1872.06";
    static final String capeForbid20_25 = "1871.12, 1871.11, 1871.1, 1871.01,   1872.07, 1872.08,    2014.04";

    //IBM_unadjusted.xlsx was generated by ExcelWriterTest
    static final String ibmStockTradeResource = "IBM_unadjusted.xlsx";

    //IBM_unadjusted.xls was saved from IBM_unadjusted.xlsx
    static final String ibmStockTradeBinaryResource = "IBM_unadjusted.xls";

    static final String ibmStockTradeJobName = "org.jberet.support.io.ExcelReaderIBMTest.xml";
    static final String ibmStockTradeFullExpected = "67040, 10810, 13310, 16810, 4800, 23310, 2600, 2800, 11110,    " +
            "7790, 22870, 14460, 27000, 19990, 31400, 19740";
    static final String ibmStockTradeBinaryFullExpected = "10810, 13310, 16810, 4800, 23310, 2600, 2800, 11110,    " +
            "30860, 156100, 25000, 18180, 8200, 26490, 35780, 35080, 27180, 22630, 16800";

    static final String ibmStockTradeExpected65520_65525 = "26780, 11860, 14530, 31620, 32710, 16800";
    static final String ibmStockTradeForbid65520_65525 = "35480, 41610, 28960,   22630, 27180, 35080,     30860";

    @Test
    public void testMoviesBeanTypeFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesBeanTypeFull.out",
                "1", null, null,
                Movie.class, moviesSheetName, "0",
                MovieTest.expectFull, null);
    }

    @Test
    public void testMoviesBeanTypeFullStreaming() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesBeanTypeFullStreaming.out",
                "1", null, null,
                Movie.class, moviesSheetName, "0",
                MovieTest.expectFull, null);
    }

    @Test
    public void testMoviesMapTypeFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesMapTypeFull.out",
                "1", null, null,
                Map.class, moviesSheetName, "0",
                MovieTest.expectFull, null);
    }

    @Test
    public void testMoviesMapTypeFullStreaming() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesMapTypeFullStreaming.out",
                "1", null, null,
                Map.class, moviesSheetName, "0",
                MovieTest.expectFull, null);
    }

    @Test
    public void testMoviesBeanType2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesBeanType2_4.out",
                "2", "4", null,
                Movie.class, moviesWithBlankCellSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesListType2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesBeanType2_4.out",
                "2", "4", null,
                List.class, moviesWithBlankCellSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesListTypeStreaming2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesListTypeStreaming2_4.out",
                "2", "4", null,
                List.class, moviesWithBlankCellSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesBeanTypeStreaming2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesBeanTypeStreaming2_4.out",
                "2", "4", null,
                Movie.class, moviesWithBlankCellSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesMapType2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesMapType2_4.out",
                "2", "4", null,
                Map.class, moviesSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesMapTypeStreaming2_4() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesMapTypeStreaming2_4.out",
                "2", "4", null,
                Map.class, moviesSheetName, "0",
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMoviesBeanType1_2() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesBeanType1_2.out",
                "1", "2", null,
                Movie.class, moviesSheetName, "0",
                MovieTest.expect1_2, MovieTest.forbid1_2);
    }

    @Test
    public void testMoviesBeanTypeStreaming1_2() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesBeanTypeStreaming1_2.out",
                "1", "2", null,
                Movie.class, moviesSheetName, "0",
                MovieTest.expect1_2, MovieTest.forbid1_2);
    }

    @Test
    public void testMoviesMapType1_2() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testMoviesMapType1_2.out",
                "1", "2", null,
                Map.class, moviesSheetName, "0",
                MovieTest.expect1_2, MovieTest.forbid1_2);
    }

    @Test
    public void testMoviesMapTypeStreaming1_2() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testMoviesMapTypeStreaming1_2.out",
                "1", "2", null,
                Map.class, moviesSheetName, "0",
                MovieTest.expect1_2, MovieTest.forbid1_2);
    }


    @Test
    public void testPersonBeanType1_5() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testPersonBeanType1_5.out",
                "1", "5", null,
                Person.class, personSheetName, "0",
                CsvItemReaderWriterTest.personResourceExpect1_5, CsvItemReaderWriterTest.personResourceForbid);
    }

    @Test
    public void testPersonBeanTypeStreaming1_5() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testPersonBeanTypeStreaming1_5.out",
                "1", "5", null,
                Person.class, personSheetName, "0",
                CsvItemReaderWriterTest.personResourceExpect1_5, CsvItemReaderWriterTest.personResourceForbid);
    }

    //the blank row should be skipped without causing any error.
    @Test
    public void testPersonBeanTypeFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                personMoviesResource, "testPersonBeanTypeFull.out",
                "1", null, null,
                Person.class, personSheetName, "0",
                CsvItemReaderWriterTest.personResourceExpect, null);
    }

    @Test
    public void testPersonBeanTypeFullStreaming() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelStreamingItemReader,
                personMoviesResource, "testPersonBeanTypeFullStreaming.out",
                "1", null, null,
                Person.class, personSheetName, "0",
                CsvItemReaderWriterTest.personResourceExpect, null);
    }

    //verify .xls excel format, use fieldMapping in lieu of header, handling of formula cells
    //capeResource is xls file so cannot read with excelStreamingItemReader.
    @Test
    public void testCapeBeanTypeFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelUserModelItemReader,
                capeResource, "testCapeBeanTypeFull.out",
                "8", "1727", capeHeader,
                Cape.class, capeSheetName, null,
                capeFullExpected, null);
    }

    //read very large OOXML excel file (IBM_unadjusted.xlsx, 34,232,654 bytes, 1,048,575 rows) with ExcelStreamingItemReader
    //and output csv file (54,854,704 bytes), which is more compact than json used in other tests here.
    //job xml is different than in above tests.
    @Test
    public void testIBMStockTradeMapTypeFull() throws Exception {
        testReadWrite0(ibmStockTradeJobName, excelStreamingItemReader,
                ibmStockTradeResource, "testIBMStockTradeMapTypeFull.out",
                "1", null, ExcelWriterTest.ibmStockTradeHeader,
                Map.class, null, "0",
                ibmStockTradeFullExpected, null);
    }

    @Test
    public void testIBMStockTradeMapType65520_65525() throws Exception {
        testReadWrite0(ibmStockTradeJobName, excelStreamingItemReader,
                ibmStockTradeResource, "testIBMStockTradeMapType65520_65525.out",
                "65520", "65525", ExcelWriterTest.ibmStockTradeHeader,
                Map.class, null, "0",
                ibmStockTradeExpected65520_65525, ibmStockTradeForbid65520_65525);
    }

    //read very large binary excel file (IBM_unadjusted.xls, 6,100,480 bytes, 65,536 rows (max number of rows))
    // with ExcelEventItemReader and output csv file (3,645,979 bytes), which is more compact than json used in other tests here.
    //job xml is different than in above tests.
    @Test
    public void testIBMStockTradeBinaryBeanTypeFull() throws Exception {
        testReadWrite0(ibmStockTradeJobName, excelEventItemReader,
                ibmStockTradeBinaryResource, "testIBMStockTradeBinaryMapTypeFull.out",
                "1", null, ExcelWriterTest.ibmStockTradeHeader,
                StockTrade.class, null, "0",
                ibmStockTradeBinaryFullExpected, null);
    }

    @Test
    public void testIBMStockTradeBinaryBeanType65520_65525() throws Exception {
        testReadWrite0(ibmStockTradeJobName, excelEventItemReader,
                ibmStockTradeBinaryResource, "testIBMStockTradeBinaryMapType65520_65525.out",
                "65520", "65525", ExcelWriterTest.ibmStockTradeHeader,
                StockTrade.class, null, "0",
                ibmStockTradeExpected65520_65525, ibmStockTradeForbid65520_65525);
    }

    //read large binary excel (*.xls) file with ExcelEventItemReader, should not cause OOME
    @Test
    public void testCapeBeanTypeEventFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelEventItemReader,
                capeResource, "testCapeMapTypeEventFull.out",
                "8", "1727", capeHeader,
                Cape.class, capeSheetName, "0",
                capeFullExpected, null);
    }

    @Test
    public void testCapeBeanTypeEvent20_25() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelEventItemReader,
                capeResource, "testCapeBeanTypeEvent20_25.out",
                "20", "25", capeHeader,
                Cape.class, capeSheetName, "0",
                capeExpected20_25, capeForbid20_25);
    }

    @Test
    public void testCapeSimpleBeanTypeEventFull() throws Exception {
        testReadWrite0(excelReaderTestJobName, excelEventItemReader,
                capeSimpleResource, "testCapeSimpleBeanTypeEventFull.out",
                "1", null, capeSimpleHeader,
                Cape.class, capeSheetName, "0",
                null, null);
    }


    private void testReadWrite0(final String jobName, final String reader,
                                final String resource, final String writeResource,
                                final String start, final String end, final String header,
                                final Class<?> beanType, final String sheetName, final String headerRow,
                                final String expect, final String forbid) throws Exception {
    	final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());
        final File writeResourceFile = new File(CsvItemReaderWriterTest.tmpdir, writeResource);
        params.setProperty("writeResource", writeResourceFile.getPath());
        params.setProperty("resource", resource);

        if (reader != null) {
            params.setProperty("reader", reader);
        } else {
            throw new IllegalArgumentException("reader parameter is not specified when starting the job");
        }
        if (header != null) {
            params.setProperty("header", header);
        }
        if (sheetName != null) {
            params.setProperty("sheetName", sheetName);
        }
        if (headerRow != null) {
            params.setProperty("headerRow", headerRow);
        }

        if (start != null) {
            params.setProperty(CsvProperties.START_KEY, start);
        }
        if (end != null) {
            params.setProperty(CsvProperties.END_KEY, end);
        }

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.HOURS);
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());

        CsvItemReaderWriterTest.validate(writeResourceFile, expect, forbid);
    }
}
