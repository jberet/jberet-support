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

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jberet.runtime.JobExecutionImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import jakarta.batch.operations.JobOperator;
import jakarta.batch.runtime.BatchRuntime;
import jakarta.batch.runtime.BatchStatus;

/**
 * A test class that reads resource from MongoDB and writes to another MongoDB collection.
 */
public final class MongoItemReaderTest {
    static MongoClient mongoClient;
    static MongoDatabase db;
    static final String jobName = "org.jberet.support.io.MongoItemReaderTest";
    private final JobOperator jobOperator = BatchRuntime.getJobOperator();

    static final String databaseName = "testData";
    static final String mongoClientUri = "mongodb://localhost/" + databaseName;
    static final String movieCollection = "movies";
    static final String movieOutCollection = "movies.out";
    static final String githubDataCollection = "githubData";
    static final String githubDataOutCollection = "githubData.out";

    @BeforeClass
    public static void beforeClass() {
        mongoClient = (MongoClient) Mongo.Holder.singleton().connect(new MongoClientURI(mongoClientUri));
        db = mongoClient.getDatabase(databaseName);
    }

    @Before
    public void before() throws Exception {
        dropCollection(movieOutCollection);
        addTestData(JsonItemReaderTest.movieJson, movieCollection, 100);
    }

    @Test
    public void testMongoGithubData() throws Exception {
        dropCollection(githubDataOutCollection);
        addTestData(JsonItemReaderTest.githubJson, githubDataCollection, 5);
        testReadWrite0(mongoClientUri, null, null, -1,
                GithubData.class, "{_id : 0}",
                githubDataCollection, githubDataOutCollection,
                null, null);
    }

    @Test
    public void testMongoMovieBeanTypeLimit2() throws Exception {
        testReadWrite0(mongoClientUri, null, "2", 2,
                Movie.class, null,
                movieCollection, movieOutCollection,
                MovieTest.expect1_2, MovieTest.forbid1_2);
    }

    @Test
    public void testMongoMovieBeanTypeLimit3Skip1() throws Exception {
        testReadWrite0(null, "1", "3", 3,
                Movie.class, null,
                movieCollection, movieOutCollection,
                MovieTest.expect2_4, MovieTest.forbid2_4);
    }

    @Test
    public void testMongoMovieBeanTypeFull() throws Exception {
        testReadWrite0(null, null, null, 100,
                Movie.class, null,
                movieCollection, movieOutCollection,
                MovieTest.expectFull, null);
    }

    private void testReadWrite0(final String uri, final String skip, final String limit, final int size,
                                final Class<?> beanType, final String projection,
                                final String collection, final String collectionOut,
                                final String expect, final String forbid) throws Exception {
        final Properties params = CsvItemReaderWriterTest.createParams(CsvProperties.BEAN_TYPE_KEY, beanType.getName());
        params.setProperty("collection", collection);
        params.setProperty("collection.out", collectionOut);
        if (uri != null) {
            params.setProperty("uri", uri);
        }
        if (projection != null) {
            params.setProperty("projection", projection);
        }
        if (skip != null) {
            params.setProperty("skip", skip);
        }
        if (limit != null) {
            params.setProperty("limit", limit);
        }

        final long jobExecutionId = jobOperator.start(jobName, params);
        final JobExecutionImpl jobExecution = (JobExecutionImpl) jobOperator.getJobExecution(jobExecutionId);
        jobExecution.awaitTermination(CsvItemReaderWriterTest.waitTimeoutMinutes, TimeUnit.MINUTES);
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getBatchStatus());

        validate(size, expect, forbid);
    }

    static void dropCollection(final String coll) {
        final MongoCollection<DBObject> collection = db.getCollection(coll, DBObject.class);
        collection.drop();
    }

    static void addTestData(final String dataResource, final String mongoCollection, final int minSizeIfExists) throws Exception {
        final MongoCollection<DBObject> collection = db.getCollection(mongoCollection, DBObject.class);
        if (collection.countDocuments() >= minSizeIfExists) {
            System.out.printf("The readCollection %s already contains 100 items, skip adding test data.%n", mongoCollection);
            return;
        }

        InputStream inputStream = MongoItemReaderTest.class.getClassLoader().getResourceAsStream(dataResource);
        if (inputStream == null) {
            try {
                final URL url = new URI(dataResource).toURL();
                inputStream = url.openStream();
            } catch (final Exception e) {
                System.out.printf("Failed to convert dataResource %s to URL: %s%n", dataResource, e);
            }
        }
        if (inputStream == null) {
            throw new IllegalStateException("The inputStream for the test data is null");
        }

        final JsonFactory jsonFactory = new MappingJsonFactory();
        final JsonParser parser = jsonFactory.createParser(inputStream);
        final JsonNode arrayNode = parser.readValueAs(ArrayNode.class);

        final Iterator<JsonNode> elements = arrayNode.elements();
        final List<DBObject> dbObjects = new ArrayList<>();
        while (elements.hasNext()) {
            final String jsonString = elements.next().toString().trim();
            if(!jsonString.isEmpty()) {
                final DBObject dbObject = BasicDBObject.parse(jsonString);
                dbObjects.add(dbObject);
            }
        }
        collection.insertMany(dbObjects);
    }

    static void validate(final int size, final String expect, final String forbid) {
        final MongoCollection<DBObject> collection = db.getCollection(movieOutCollection, DBObject.class);
        final MongoCursor<DBObject> cursor = collection.find().iterator();

        try {
            //if size is negative number, it means the size is unknown and so skip the size check.
            if (size >= 0) {
                Assert.assertEquals(size, collection.countDocuments());
            }
            final List<String> expects = new ArrayList<>();
            String[] forbids = CellProcessorConfig.EMPTY_STRING_ARRAY;
            if (expect != null && !expect.isEmpty()) {
                Collections.addAll(expects, expect.split(","));
            }
            if (forbid != null && !forbid.isEmpty()) {
                forbids = forbid.split(",");
            }
            if (expects.size() == 0 && forbids.length == 0) {
                return;
            }
            while (cursor.hasNext()) {
                final DBObject next = cursor.next();
                final String stringValue = next.toString();
                for (final String s : forbids) {
                    if (stringValue.contains(s.trim())) {
                        throw new IllegalStateException("Forbidden string found: " + s);
                    }
                }
                for (final Iterator<String> it = expects.iterator(); it.hasNext(); ) {
                    final String s = it.next();
                    if (stringValue.contains(s.trim())) {
                        System.out.printf("Found expected string: %s%n", s);
                        it.remove();
                    }
                }
            }
            if (expects.size() > 0) {
                throw new IllegalStateException("Some expected strings are still not found: " + expects);
            }
        } finally {
            cursor.close();
        }
    }
}
