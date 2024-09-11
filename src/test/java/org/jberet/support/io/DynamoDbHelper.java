package org.jberet.support.io;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assumptions.assumeTrue;


class DynamoDbHelper {
    static final String ENDPOINT_URI = "http://localhost:8000";
    static final String ACCESS_KEY_ID = "JBeret";
    static final String SECRET_ACCESS_KEY = "JBeret";
    public static final String TABLE_NAME = "stock_trade";
    DynamoDbClient client;
    DynamoDbEnhancedClient enhancedClient;
    DynamoDbTable<StockTradeDynamoDb> table;
    static Boolean available = null;

    public static boolean isDynamoDbLocalAvailable() {
        if (available == null) {
            try {
                HttpURLConnection connection = (HttpURLConnection) new URL(ENDPOINT_URI).openConnection();
                connection.setConnectTimeout(1000);
                available = connection.getResponseCode() > 0 && connection.getResponseCode() < 600;
            } catch (IOException e) {
                available = false;
            }
        }
        return available;
    }

    public static void assumeDynamoDbLocalAvailable() {
        assumeTrue(DynamoDbHelper.isDynamoDbLocalAvailable(), "DynamoDB local should be running and listening at " + ENDPOINT_URI);
    }

    public void setUp() {
        if (!isDynamoDbLocalAvailable()) {
            return;
        }
        client = DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT_URI))
                .credentialsProvider(() -> AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY))
                .region(Region.EU_WEST_1)
                .build();
        enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(client)
                .build();
        table = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(StockTradeDynamoDb.class));
    }

    public void createTable() {
        try {
            table.createTable();
        } catch (DynamoDbException e) {
            // Table already exists
        }
    }

    public void deleteTable() {
        try {
            table.deleteTable();
        } catch (DynamoDbException e) {
            // Table not exists
        }
    }

    public void tearDown() {
        if (!isDynamoDbLocalAvailable()) {
            return;
        }
        try {
            table.deleteTable();
        } catch (DynamoDbException e) {
            // Table does not exist
        }
    }

    static List<StockTradeDynamoDb> loadItems(int limit) {
        try (Stream<String> lineStream = Files.lines(Path.of(StockTradeDynamoDb.class.getResource("/IBM_unadjusted.txt").toURI()))) {
            return lineStream
                    .filter(StringUtils::isNotBlank)
                    .map(StockTradeDynamoDb::parseCsv)
                    .limit(limit)
                    .collect(Collectors.toList());
        } catch (IOException | URISyntaxException e) {
            throw new IllegalStateException("Failed to load IBM_unadjusted.txt");
        }
    }

    List<StockTradeDynamoDb> loadAndPutItems(int limit) {
        List<StockTradeDynamoDb> loadedItems = DynamoDbHelper.loadItems(limit);
        loadedItems.forEach(getTable()::putItem);
        return loadedItems;
    }

    public DynamoDbEnhancedClient getEnhancedClient() {
        return enhancedClient;
    }

    public DynamoDbTable<StockTradeDynamoDb> getTable() {
        return table;
    }
}
