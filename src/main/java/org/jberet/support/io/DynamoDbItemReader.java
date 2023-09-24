package org.jberet.support.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.batch.api.BatchProperty;
import jakarta.batch.api.chunk.ItemReader;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * An implementation of {@code jakarta.batch.api.chunk.ItemRead} that scans or queries items from DynamoDB.
 */
@Named
@Dependent
public class DynamoDbItemReader<D> extends DynamoDbItemReadWriterBase<D> implements ItemReader {
	/**
	 * When indexName is set, the given index is read.
	 * When indexName is not set, the whole table is read.
	 */
	@Inject
	@BatchProperty
	protected String indexName;
	/**
	 * When partition key is set, a query on given partition is issued.
	 * When partition key is not set, a scan on the whole table is issued.
	 */
	@Inject
	@BatchProperty
	protected String partitionKey;
	/**
	 * Consistent read flag
	 */
	@Inject
	@BatchProperty
	protected Boolean consistentRead;
	/**
	 * Number of items to skip at the beginning
	 */
	@Inject
	@BatchProperty
	protected Integer start;
	/**
	 * Maximum number of items to retrieve
	 */
	@Inject
	@BatchProperty
	protected Integer end;
	/**
	 * Maximum number of items to read on server side
	 */
	@Inject
	@BatchProperty
	protected Integer limit;
	/**
	 * Filter expression.
	 * Example: <code>price>=:minPrice and active=:active and #type=:type</code>
	 */
	@Inject
	@BatchProperty
	protected String filterExpression;
	/**
	 * Field nama aliases to inject in the {@link #filterExpression}.
	 * This is a key-value map encoded in JSON.
	 * Example: <code>{"#type":"type"}</code>
	 */
	@Inject
	@BatchProperty
	protected String filterExpressionNames;
	/**
	 * Values to inject in the {@link #filterExpression}.
	 * This is a key-value map encoded in JSON.
	 * Example: <code>{":minPrice":12,":active":true,":type":"candy"}</code>
	 */
	@Inject
	@BatchProperty
	protected String filterExpressionValues;
	/**
	 * Attributes to project in result bean.
	 * This is a coma separated list of field names.
	 * Example: <code>name,minPrice,active,type</code>
	 */
	@Inject
	@BatchProperty
	protected String attributesToProject;
	/**
	 * Current item iterator
	 */
	private Iterator<D> itemIterator;

	@Override
	public void open(Serializable serializable) throws Exception {
		initEnhancedClient();
		PageIterable<D> pageIterable = read(getTable());
		Stream<D> itemStream = pageIterable.items().stream();
		if (this.start != null && this.start >= 0) {
			itemStream = itemStream.skip(this.start);
		}
		if (this.end != null && this.end > 0) {
			itemStream = itemStream.limit(this.end - (this.start == null ? 0 : this.start));
		}
		itemIterator = itemStream.iterator();
	}

	private PageIterable<D> read(DynamoDbTable<D> table) {
		Key queryKey = getQueryKey();
		PageIterable<D> pageIterable;
		if (this.indexName == null) {
			if (queryKey == null) {
				pageIterable = table.scan(this::configureScanRequest);
			} else {
				pageIterable = table.query(builder -> configureQueryRequest(builder, queryKey));
			}
		} else {
			DynamoDbIndex<D> index = table.index(this.indexName);
			if (queryKey == null) {
				pageIterable = PageIterable.create(index.scan(this::configureScanRequest));
			} else {
				pageIterable = PageIterable.create(index.query(builder -> configureQueryRequest(builder, queryKey)));
			}
		}
		return pageIterable;
	}

	private QueryEnhancedRequest.Builder configureQueryRequest(QueryEnhancedRequest.Builder builder, Key queryKey) {
		return builder
				.consistentRead(this.consistentRead)
				.limit(this.limit)
				.queryConditional(QueryConditional.keyEqualTo(queryKey))
				.filterExpression(getFilterExpression());
	}

	private ScanEnhancedRequest.Builder configureScanRequest(ScanEnhancedRequest.Builder builder) {
		return builder
				.consistentRead(this.consistentRead)
				.limit(this.limit)
				.filterExpression(getFilterExpression())
				.attributesToProject(getAttributesToProject());
	}


	@Override
	public void close() throws Exception {

	}

	@Override
	public D readItem() {
		return itemIterator.hasNext() ? itemIterator.next() : null;
	}

	private Expression getFilterExpression() {
		if (this.filterExpression == null) {
			return null;
		} else {
			return Expression.builder()
					.expression(this.filterExpression)
					.expressionNames(getFilterExpressionNames())
					.expressionValues(getFilterExpressionValues())
					.build();
		}
	}

	protected Map<String, String> getFilterExpressionNames() {
		if (this.filterExpressionNames == null) {
			return null;
		}
		try {
			return new ObjectMapper().readValue(this.filterExpressionNames, new TypeReference<>() {
			});
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Invalid filterExpressionNames JSON", e);
		}
	}

	protected Map<String, AttributeValue> getFilterExpressionValues() {
		if (this.filterExpressionValues == null) {
			return Collections.emptyMap();
		}
		try {
			ObjectNode objectNode = (ObjectNode) new ObjectMapper().readTree(this.filterExpressionValues);
			return getAttributeValues(objectNode);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Invalid filterExpressionValues JSON", e);
		}
	}

	static Map<String, AttributeValue> getAttributeValues(ObjectNode objectNode) {
		Map<String, AttributeValue> map = new HashMap<>(objectNode.size());
		Iterator<Map.Entry<String, JsonNode>> fieldIter = objectNode.fields();
		while (fieldIter.hasNext()) {
			Map.Entry<String, JsonNode> field = fieldIter.next();
			map.put(field.getKey(), getAttributeValue(field.getValue()));
		}
		return map;
	}

	private static List<AttributeValue> getAttributeValues(ArrayNode arrayNode) {
		List<AttributeValue> list = new ArrayList<>(arrayNode.size());
		for (JsonNode item : arrayNode) {
			list.add(getAttributeValue(item));
		}
		return list;
	}

	private static AttributeValue getAttributeValue(JsonNode node) {
		if (node == null || node.isNull()) {
			return AttributeValue.fromNul(true);
		} else if (node.isTextual()) {
			return AttributeValue.fromS(node.textValue());
		} else if (node.isNumber()) {
			return AttributeValue.fromN(node.toString());
		} else if (node.isBoolean()) {
			return AttributeValue.fromBool(node.booleanValue());
		} else if (node.isArray()) {
			return AttributeValue.fromL(getAttributeValues((ArrayNode) node));
		} else if (node.isObject()) {
			return AttributeValue.fromM(getAttributeValues((ObjectNode) node));
		}
		return null;
	}

	private Collection<String> getAttributesToProject() {
		if (this.attributesToProject == null) {
			return null;
		}
		return Stream.of(this.attributesToProject.split(","))
				.map(String::trim).collect(Collectors.toList());
	}

	private Key getQueryKey() {
		if (partitionKey == null) {
			return null;
		}
		return Key.builder().partitionValue(partitionKey).build();
	}

	@Override
	public Serializable checkpointInfo() throws Exception {
		return null;
	}
}
