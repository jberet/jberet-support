package org.jberet.support.io;

import jakarta.batch.api.BatchProperty;
import jakarta.batch.api.chunk.ItemWriter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@code jakarta.batch.api.chunk.ItemWriter} that puts or deletes items to DynamoDB.
 */
@Named
@Dependent
public class DynamoDbItemWriter<D> extends DynamoDbItemReadWriterBase<D> implements ItemWriter {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Flag to indicate whether items should be created/update or deleted
	 */
	@Inject
	@BatchProperty
	protected boolean deleteItem;

	/**
	 * The maximum number of items per write batch is 25.
	 * To avoid hitting this limit, items are writen making multiple requests of maximum 25 items.
	 *
	 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html">APIBatchWriteItem documentation</a>
	 */
	private class InternalWriteBatch {
		private final DynamoDbTable<D> table;
		private WriteBatch.Builder<D> batchBuilder;
		private int batchSize;
		private final List<D> unprocessedPutItems = new ArrayList<>();
		private final List<Key> unprocessedDeleteItems = new ArrayList<>();

		private InternalWriteBatch() {
			this.table = getTable();
			newBatch();
		}

		private void newBatch() {
			this.batchBuilder = WriteBatch.builder(beanClass).mappedTableResource(table);
			this.batchSize = 0;
		}

		void addItem(Object item) {
			addItemToBatch(item, this.batchBuilder);
			this.batchSize++;
			if (this.batchSize == 25) {
				flush();
			}
		}

		void flush() {
			if (this.batchSize == 0) {
				return;
			}
			// Execute batch
			BatchWriteResult batchWriteResult = enhancedClient.batchWriteItem(BatchWriteItemEnhancedRequest.builder()
					.addWriteBatch(batchBuilder.build())
					.build());
			// Check result
			unprocessedPutItems.addAll(batchWriteResult.unprocessedPutItemsForTable(table));
			unprocessedDeleteItems.addAll(batchWriteResult.unprocessedDeleteItemsForTable(table));
			newBatch();
		}

	}

	/**
	 * Converts an item into batch element, either of type putItem or deleteItem.
	 */
	protected void addItemToBatch(Object item, WriteBatch.Builder<D> writeBatch) {
		D typedItem = beanClass.cast(item);
		if (shouldDeleteItem(typedItem)) {
			writeBatch.addDeleteItem(typedItem);
		} else {
			writeBatch.addPutItem(typedItem);
		}
	}

	/**
	 * Determines whether item should deleted or put to batch.
	 * This method can be overriden.
	 */
	protected boolean shouldDeleteItem(D item) {
		return deleteItem;
	}

	@Override
	public void open(Serializable serializable) {
		initEnhancedClient();
	}

	@Override
	public void writeItems(List<Object> items) {
		InternalWriteBatch batch = new InternalWriteBatch();
		items.forEach(batch::addItem);
		batch.flush();
		if (!batch.unprocessedPutItems.isEmpty()) {
			handleUnprocessedPutItems(batch.unprocessedPutItems);
		}
		if (!batch.unprocessedDeleteItems.isEmpty()) {
			handleUnprocessedDeleteItems(batch.unprocessedDeleteItems);
		}
	}

	/**
	 * Handles unprocessed delete items.
	 * By default, unprocessed items are logged.
	 * This method can be overriden.
	 */
	protected void handleUnprocessedDeleteItems(List<Key> unprocessedDeleteItems) {
		logger.warn("{} unprocessed delete items", unprocessedDeleteItems.size());
	}

	/**
	 * Handles unprocessed put items.
	 * By default, unprocessed items are logged.
	 * This method can be overriden.
	 */
	protected void handleUnprocessedPutItems(List<D> unprocessedPutItems) {
		logger.warn("{} unprocessed put items", unprocessedPutItems.size());
	}

	@Override
	public Serializable checkpointInfo() throws Exception {
		return null;
	}

	@Override
	public void close() throws Exception {

	}

}
