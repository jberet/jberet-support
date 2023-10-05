package org.jberet.support.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;
import org.jberet.runtime.JobExecutionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.jberet.support.io.DynamoDbHelper.*;
import static org.junit.Assert.*;

/**
 * Tests for {@link DynamoDbItemReader}.
 * This test requires a DynamoDB local to be running.
 *
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html">DynamoDBLocal Guide</a>
 * @see <a href="https://hub.docker.com/r/amazon/dynamodb-local">DynamoDBLocal Docker image</a>
 */
public class DynamoDbItemReaderTest {
	static final String DATE_KEY = "01/02/1998";
	static final JobOperator jobOperator = BatchRuntime.getJobOperator();
	final DynamoDbHelper helper = new DynamoDbHelper();

	@Before
	public void setUp() {
		helper.setUp();
        helper.createTable();
	}

	@After
	public void tearDown() {
		helper.tearDown();
	}

	static DynamoDbItemReader<StockTradeDynamoDb> createReader() {
		DynamoDbItemReader<StockTradeDynamoDb> reader = new DynamoDbItemReader<>();
		reader.endpointUri = ENDPOINT_URI;
		reader.accessKeyId = ACCESS_KEY_ID;
		reader.secretAccessKey = SECRET_ACCESS_KEY;
		reader.region = Region.EU_WEST_1.id();
		reader.tableName = TABLE_NAME;
		reader.beanClass = StockTradeDynamoDb.class;
		return reader;
	}

	private static List<StockTradeDynamoDb> readAll(DynamoDbItemReader<StockTradeDynamoDb> reader) {
		List<StockTradeDynamoDb> readItems = new ArrayList<>();
		StockTradeDynamoDb readItem;
		while ((readItem = reader.readItem()) != null) {
			readItems.add(readItem);
		}
		return readItems;
	}

	@Test
	public void testReadItemsWithScan() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(123);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(loadedItems, readItems);
	}

	@Test
	public void testReadItemsWithScanAndBounds() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(60);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.start=10;
		reader.end=20;
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(10, readItems.size());
	}

	@Test
	public void testReadItemsWithScanAndProject() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(12);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.attributesToProject="dateKey,time,volume";
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(12, readItems.size());
		for(StockTradeDynamoDb item: readItems) {
			// Fetched
			assertNotNull(item.getDateKey());
			assertNotNull(item.getTime());
			assertNotEquals(0.0D, item.getVolume(), 0.1D);
			// Not fetched
			assertEquals(0.0D, item.getHigh(), 0.1D);
			assertEquals(0.0D, item.getLow(), 0.1D);
		}
	}

	@Test
	public void testReadItemsWithScanAndFilter() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(105);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.filterExpression = "volume >= :minVolume";
		reader.filterExpressionValues = "{\":minVolume\":10000}";
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(loadedItems.stream().filter(i -> i.getVolume() >= 10000).collect(Collectors.toList()), readItems);
	}

	@Test
	public void testReadItemsWithQuery() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(134);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.partitionKey = DATE_KEY;
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(loadedItems.stream().filter(i -> i.getDateKey().equals(DATE_KEY)).collect(Collectors.toList()), readItems);
	}

	@Test
	public void testReadItemsWithQueryAndFilter() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(151);
		// Initialize Reader
		DynamoDbItemReader<StockTradeDynamoDb> reader = createReader();
		reader.partitionKey = DATE_KEY;
		reader.filterExpression = "volume >= :minVolume";
		reader.filterExpressionValues = "{\":minVolume\":10000}";
		reader.open(null);

		// Read items to dynamo
		List<StockTradeDynamoDb> readItems = readAll(reader);

		// Check dynamo content
		assertEquals(loadedItems.stream().filter(i -> i.getDateKey().equals(DATE_KEY) && i.getVolume() >= 10000).collect(Collectors.toList()), readItems);
	}

	/**
	 * Run Job in org.jberet.support.io.DynamoDbReaderTest.xml
	 */
	void runJob(int start, int end) throws Exception {
		Properties jobParams = new Properties();
		jobParams.setProperty("start", String.valueOf(start));
		jobParams.setProperty("end", String.valueOf(end));
		final long jobExecutionId = jobOperator.start("org.jberet.support.io.DynamoDbReaderTest", jobParams);
		final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
		jobExecution.awaitTermination(1, TimeUnit.MINUTES);
		assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());
	}

	@Test
	public void testRunReadJob() throws Exception {
		assumeDynamoDbLocalAvailable();
        List<StockTradeDynamoDb> loadedItems = helper.loadAndPutItems(321);
		// Run job
		runJob(0, 321);
	}

	@Test
	public void testGetAttributeValues() throws JsonProcessingException {
		ObjectNode objectNode = (ObjectNode) new ObjectMapper()
				.readTree("{\":number\":12,\":boolean\":true,\":string\":\"candy\",\":null\": null,\":array\":[1,2,3],\":object\":{\"a\":1,\"b\":2}}");
		Map<String, AttributeValue> attributeValues = DynamoDbItemReader.getAttributeValues(objectNode);
		assertEquals(12, Integer.parseInt(attributeValues.get(":number").n()));
		assertTrue(attributeValues.get(":boolean").bool().booleanValue());
		assertEquals("candy", attributeValues.get(":string").s());
		assertTrue(attributeValues.get(":null").nul().booleanValue());
		assertEquals(Arrays.asList(1,2,3), attributeValues.get(":array").l().stream().map(AttributeValue::n).map(Integer::parseInt).collect(Collectors.toList()));
		Map<String, Integer> map = attributeValues.get(":object").m().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.valueOf(e.getValue().n())));
		assertEquals(2, map.size());
		assertEquals(1, map.get("a").intValue());
		assertEquals(2, map.get("b").intValue());
	}

}
