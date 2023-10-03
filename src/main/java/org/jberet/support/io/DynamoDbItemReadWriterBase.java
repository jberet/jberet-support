package org.jberet.support.io;

import jakarta.batch.api.BatchProperty;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

/**
 * The base class for {@link DynamoDbItemReader} and {@link DynamoDbItemWriter}.
 *
 * @see DynamoDbItemReader
 * @see DynamoDbItemWriter
 */
public class DynamoDbItemReadWriterBase<D> {
    /**
     * CDI provided {@link DynamoDbEnhancedClient}
     */
    @Inject
    protected Instance<DynamoDbClient> clientInstance;

    /**
     * CDI provided {@link DynamoDbEnhancedClient}
     */
    @Inject
    protected Instance<DynamoDbEnhancedClient> enhancedClientInstance;

    /**
     * Dynamo DB endpoint URI.
     * Only when no {@link DynamoDbEnhancedClient} is provided, mainly for DynamoDb Local testing
     */
    @Inject
    @BatchProperty
    protected String endpointUri;
    /**
     * Dynamo DB access key Id.
     * By default, connection to DynamoDb uses default credential provided: environment variables,
     * Java system properties, and AWS profile are supported.
     * Only when no {@link DynamoDbEnhancedClient} is provided, mainly for DynamoDb Local testing
     */
    @Inject
    @BatchProperty
    protected String accessKeyId;
    /**
     * Dynamo DB secret access key.
     * By default, connection to DynamoDb uses default credential provided: environment variables,
     * Java system properties, and AWS profile are supported.
     * Only when no {@link DynamoDbEnhancedClient} is provided, mainly for DynamoDb Local testing
     */
    @Inject
    @BatchProperty
    protected String secretAccessKey;
    /**
     * AWS region, see {@link  Region}
     */
    @Inject
    @BatchProperty
    protected String region;
    /**
     * Effective instance of {@link DynamoDbClient}
     */
    protected DynamoDbClient client;
    /**
     * Effective instance of {@link DynamoDbEnhancedClient}
     */
    protected DynamoDbEnhancedClient enhancedClient;
    /**
     * Bean class used for table / object mapping
     */
    @Inject
    @BatchProperty
    protected Class<D> beanClass;
    /**
     * Target Dynamo DB table
     */
    @Inject
    @BatchProperty
    protected String tableName;

    public DynamoDbTable<D> getTable() {
        return enhancedClient.table(tableName, TableSchema.fromBean(beanClass));
    }

    /**
     * Initialize {@link #client} and {@link #enhancedClient} field either using {@link #clientInstance} and {@link #enhancedClientInstance}
     * or creating a specific one.
     */
    protected void initEnhancedClient() {
        if (enhancedClient == null) {
            if (enhancedClientInstance == null || enhancedClientInstance.isUnsatisfied()) {
                initClient();
                enhancedClient = DynamoDbEnhancedClient.builder()
                        .dynamoDbClient(client)
                        .build();
            } else {
                // Externally provider client
                enhancedClient = enhancedClientInstance.get();
            }
        }
    }

    /**
     * Initialize {@link #client}  field either using {@link #clientInstance}
     * or creating a specific one.
     */
    protected void initClient() {
        if (client == null) {
            if (clientInstance == null || clientInstance.isUnsatisfied()) {
                DynamoDbClientBuilder baseClientBuilder = DynamoDbClient.builder();
                if (endpointUri != null) {
                    baseClientBuilder = baseClientBuilder.endpointOverride(URI.create(endpointUri));
                }
                if (accessKeyId != null && secretAccessKey != null) {
                    baseClientBuilder = baseClientBuilder.credentialsProvider(this::awsBasicCredentials);
                }
                if (region != null) {
                    baseClientBuilder = baseClientBuilder.region(Region.of(region));
                }
                client = baseClientBuilder.build();
            } else {
                // Externally provider client
                client = clientInstance.get();
            }

        }
    }

    private AwsBasicCredentials awsBasicCredentials() {
        return AwsBasicCredentials.create(this.accessKeyId, this.secretAccessKey);
    }
}