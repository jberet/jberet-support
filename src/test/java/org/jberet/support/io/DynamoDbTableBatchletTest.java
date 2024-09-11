package org.jberet.support.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import static org.jberet.support.io.DynamoDbHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamoDbTableBatchletTest {

    final DynamoDbHelper helper = new DynamoDbHelper();

    @BeforeEach
    public void setUp() {
        helper.setUp();
    }

    @AfterEach
    public void tearDown() {
        helper.tearDown();
    }

    private TableDescription describeTable() {
        return helper.client.describeTable(DescribeTableRequest.builder().tableName(DynamoDbHelper.TABLE_NAME).build()).table();
    }

    private DynamoDbTableBatchlet<StockTradeDynamoDb> newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action action) {
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = new DynamoDbTableBatchlet<>();
        batchlet.endpointUri = ENDPOINT_URI;
        batchlet.accessKeyId = ACCESS_KEY_ID;
        batchlet.secretAccessKey = SECRET_ACCESS_KEY;
        batchlet.region = Region.EU_WEST_1.id();
        batchlet.tableName = TABLE_NAME;
        batchlet.beanClass = StockTradeDynamoDb.class;
        batchlet.action = action.name();
        return batchlet;
    }

    @Test
    public void create() {
        // Given
        helper.deleteTable();
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.CREATE);
        // When
        batchlet.process();
        // Then
        TableDescription tableDescription = describeTable();
        assertEquals(TABLE_NAME, tableDescription.tableName());
        assertEquals(BillingMode.PAY_PER_REQUEST, tableDescription.billingModeSummary().billingMode());
    }

    @Test
    public void createProvisioned() {
        // Given
        helper.deleteTable();
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.CREATE);
        batchlet.readCapacityUnits = 2L;
        batchlet.writeCapacityUnits = 1L;
        // When
        batchlet.process();
        // Then
        TableDescription tableDescription = describeTable();
        assertEquals(TABLE_NAME, tableDescription.tableName());
        assertEquals(2L, tableDescription.provisionedThroughput().readCapacityUnits().longValue());
        assertEquals(1L, tableDescription.provisionedThroughput().writeCapacityUnits().longValue());
        //assertEquals(BillingMode.PROVISIONED, tableDescription.billingModeSummary().billingMode());
    }

    @Test
    public void createTwice() {
        // Given
        helper.deleteTable();
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.CREATE);
        batchlet.process();
        // When
        batchlet.process();
        // Then
        TableDescription tableDescription = describeTable();
        assertEquals(TABLE_NAME, tableDescription.tableName());
    }

    @Test
    public void update() {
        // Given
        helper.deleteTable();
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.CREATE);
        batchlet.process();
        // When
        batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.UPDATE);
        batchlet.process();
        // Then
        TableDescription tableDescription = describeTable();
        assertEquals(TABLE_NAME, tableDescription.tableName());
    }

    @Test
    public void truncate() {
        // Given
        helper.deleteTable();
        DynamoDbTableBatchlet<StockTradeDynamoDb> batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.CREATE);
        batchlet.process();

        helper.loadAndPutItems(12);
        assertEquals(12, helper.getTable().scan().items().stream().count());
        // When
        batchlet = newDynamoDbTableBatchlet(DynamoDbTableBatchlet.Action.TRUNCATE);
        batchlet.process();
        // Then
        assertEquals(0, helper.getTable().scan().items().stream().count());
    }

}
