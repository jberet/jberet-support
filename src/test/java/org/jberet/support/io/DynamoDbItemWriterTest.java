package org.jberet.support.io;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import org.jberet.runtime.JobExecutionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.jberet.support.io.DynamoDbHelper.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DynamoDbItemWriter}
 * This test requires a DynamoDB local to be running.
 *
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html">DynamoDBLocal Guide</a>
 * @see <a href="https://hub.docker.com/r/amazon/dynamodb-local">DynamoDBLocal Docker image</a>
 */
public class DynamoDbItemWriterTest {
    static final JobOperator jobOperator = BatchRuntime.getJobOperator();
    final DynamoDbHelper helper = new DynamoDbHelper();


    @Before
    public void setUp() {
        helper.setUp();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }

    @Test
    public void testWriteItems() {
        assumeDynamoDbLocalAvailable();
        // Initialize writer
        DynamoDbItemWriter<StockTradeDynamoDb> writer = new DynamoDbItemWriter<>();
        writer.endpointUri = ENDPOINT_URI;
        writer.accessKeyId = ACCESS_KEY_ID;
        writer.secretAccessKey = SECRET_ACCESS_KEY;
        writer.region = Region.EU_WEST_1.id();
        writer.tableName = TABLE_NAME;
        writer.beanClass = StockTradeDynamoDb.class;
        writer.open(null);

        // Load items CSV file
        List<StockTradeDynamoDb> items = loadItems(128);

        // Write items to dynamo
        writer.writeItems(items.stream().map(Object.class::cast).collect(Collectors.toList()));

        // Check dynamo content
        assertEquals(items.size(), helper.getTable().scan().items().stream().count());
    }

	@Test
	public void testDeleteItems() {
		assumeDynamoDbLocalAvailable();
		// Initialize writer
		DynamoDbItemWriter<StockTradeDynamoDb> writer = new DynamoDbItemWriter<>();
		writer.endpointUri = ENDPOINT_URI;
		writer.accessKeyId = ACCESS_KEY_ID;
		writer.secretAccessKey = SECRET_ACCESS_KEY;
		writer.region = Region.EU_WEST_1.id();
		writer.tableName = TABLE_NAME;
		writer.beanClass = StockTradeDynamoDb.class;
		writer.deleteItem = true;
		writer.open(null);

		// Load items CSV file
		List<StockTradeDynamoDb> items = loadItems(32);
		items.forEach(helper.getTable()::putItem);

		// Delete items from dynamo
		writer.writeItems(items.stream().map(Object.class::cast).collect(Collectors.toList()));


		// Check dynamo content
		assertEquals(0, helper.getTable().scan().items().stream().count());
	}

    void runJob(int start, int end) throws Exception {
        Properties jobParams = new Properties();
        jobParams.setProperty("start", String.valueOf(start));
        jobParams.setProperty("end", String.valueOf(end));
        final long jobExecutionId = jobOperator.start("org.jberet.support.io.DynamoDbWriterTest", jobParams);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(1, TimeUnit.MINUTES);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());
    }

    @Test
    public void testRunWriteJob() throws Exception {
        assumeDynamoDbLocalAvailable();
        // Run job
        // See org.jberet.support.io.DynamoDbWriterTest.xml
        runJob(0, 321);

        // Check dynamo content
        assertEquals(321, helper.getTable().scan().items().stream().count());
    }

}
