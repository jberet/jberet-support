package org.jberet.support.io;

import jakarta.batch.api.BatchProperty;
import jakarta.batch.api.Batchlet;
import jakarta.batch.runtime.BatchStatus;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named
@Dependent
public class DynamoDbTableBatchlet<D> extends DynamoDbItemReadWriterBase<D> implements Batchlet {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * Either delete, create, update or truncate
     */
    @Inject
    @BatchProperty
    String action;

    public enum Action {
        DELETE, CREATE, UPDATE, TRUNCATE
    }

    /**
     * DynamoDB Local does not support some features like point in time recovery or tags
     */
    @Inject
    @BatchProperty
    boolean dynamoDbLocal;
    @Inject
    @BatchProperty
    Long writeCapacityUnits;
    @Inject
    @BatchProperty
    Long readCapacityUnits;
    @Inject
    @BatchProperty
    List<String> globalSecondaryIndices;
    @Inject
    @BatchProperty
    String kmsMasterKeyId;
    @Inject
    @BatchProperty
    Boolean pointInTimeRecoveryEnabled;
    @Inject
    @BatchProperty
    Map<String, String> tableTags;

    private static <D> boolean tableExists(DynamoDbTable<D> table) {
        try {
            table.describeTable();
            return true;
        } catch (ResourceNotFoundException tableNotFoundExc) {
            return false;
        }
    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public String process() {
        if (action == null || action.isEmpty()) {
            return BatchStatus.COMPLETED.name();
        }
        initClient();
        initEnhancedClient();
        DynamoDbTable<D> table = getTable();
        boolean tableExists = tableExists(table);
        switch (Action.valueOf(action.toUpperCase())) {
            case DELETE:
                if (tableExists) {
                    doDelete(table);
                }
                break;
            case CREATE:
                if (!tableExists) {
                    doCreate(table);
                }
                doUpdate();
                break;
            case UPDATE:
                if (tableExists) {
                    doUpdate();
                }
                break;
            case TRUNCATE:
                if (tableExists) {
                    doDelete(table);
                }
                doCreate(table);
                doUpdate();
        }
        return BatchStatus.COMPLETED.name();
    }

    private void doDelete(DynamoDbTable<D> table) {
        logger.warn("Deleting table {}", tableName);
        table.deleteTable();
        client.waiter().waitUntilTableNotExists(DescribeTableRequest.builder().tableName(tableName).build());
    }

    private void doCreate(DynamoDbTable<D> table) {
        logger.warn("Creating table {}", tableName);
        CreateTableEnhancedRequest.Builder createRequest = CreateTableEnhancedRequest.builder();
        if (globalSecondaryIndices != null && !globalSecondaryIndices.isEmpty()) {
            createRequest.globalSecondaryIndices(globalSecondaryIndices.stream()
                    .map(DynamoDbTableBatchlet::getEnhancedGlobalSecondaryIndex)
                    .collect(Collectors.toList()));
        }
        if (isValidCapacityUnits(writeCapacityUnits) && isValidCapacityUnits(readCapacityUnits)) {
            createRequest.provisionedThroughput(b ->
                    b.writeCapacityUnits(writeCapacityUnits).readCapacityUnits(readCapacityUnits));
        }
        table.createTable(createRequest.build());
        client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build());
    }

    private static boolean isValidCapacityUnits(Long capacityUnits) {
        return capacityUnits != null && capacityUnits > 0;
    }

    private void doUpdate() {
        logger.warn("Updating table {}", tableName);
        String tableArn = null;
        if (kmsMasterKeyId != null && !kmsMasterKeyId.isEmpty()) {
            UpdateTableRequest.Builder updateRequest = UpdateTableRequest.builder()
                    .tableName(tableName);
            updateRequest = updateRequest.sseSpecification(SSESpecification.builder()
                    .enabled(true)
                    .sseType(SSEType.KMS)
                    .kmsMasterKeyId(kmsMasterKeyId)
                    .build());
            UpdateTableResponse updateResponse = client.updateTable(updateRequest.build());
            tableArn = updateResponse.tableDescription().tableArn();
        }
        // Tags
        if (tableTags != null && !tableTags.isEmpty() && !dynamoDbLocal) {
            if (tableArn == null) {
                DescribeTableResponse describeResponse = client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
                tableArn = describeResponse.table().tableArn();
            }
            List<Tag> tags = tableTags.entrySet().stream()
                    .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
                    .collect(Collectors.toList());
            client.tagResource(TagResourceRequest.builder()
                    .resourceArn(tableArn)
                    .tags(tags)
                    .build());
        }
        // Point in time recovery
        if (pointInTimeRecoveryEnabled != null && !dynamoDbLocal) {
            client.updateContinuousBackups(UpdateContinuousBackupsRequest.builder()
                    .tableName(tableName)
                    .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                            .pointInTimeRecoveryEnabled(true)
                            .build())
                    .build());
        }
    }

    private static EnhancedGlobalSecondaryIndex getEnhancedGlobalSecondaryIndex(String indexName) {
        return EnhancedGlobalSecondaryIndex.builder()
                .indexName(indexName)
                .projection(pr -> pr.projectionType(ProjectionType.ALL))
                .build();
    }
}
