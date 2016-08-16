/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.datastore.v1.demos.guestbook;

import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreHelper;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * A command-line guestbook demo application using the datastore API.
 */
public class Guestbook {
  private static final String GREETING_KIND = "greeting";
  private static final String GUESTBOOK_KIND = "guestbook";
  private static final String KEY_PROPERTY = "__key__";
  private static final String USER_PROPERTY = "user";
  private static final String DATE_PROPERTY = "date";
  private static final String MESSAGE_PROPERTY = "message";

  private final String[] args;
  private Datastore datastore;

  public Guestbook(String[] args) {
    this.args = args;
  }

  public static void main(String[] args) {
    new Guestbook(args).run();
  }

  public void run() {
    try {
      datastore = DatastoreHelper.getDatastoreFromEnv();
    } catch (GeneralSecurityException exception) {
      System.err.println("Security error connecting to the datastore: " + exception.getMessage());
      System.exit(2);
    } catch (IOException exception) {
      System.err.println("I/O error connecting to the datastore: " + exception.getMessage());
      System.exit(2);
    }

    try {
      checkUsage(args.length > 0);
      if (args[0].equalsIgnoreCase("list")) {
        checkUsage(args.length == 2);
        listGreetings(args[1]);
      } else if (args[0].equalsIgnoreCase("greet")) {
        checkUsage(args.length == 4);
        addGreeting(args[1], args[2], args[3]);
      } else {
        checkUsage(false);
      }
    } catch (DatastoreException exception) {
      System.err.println("Error talking to the datastore: " + exception.getMessage());
      System.exit(2);
    }
  }

  /**
   * Print usage message and exit if ok is false.
   */
  private void checkUsage(boolean ok) {
    if (!ok) {
      System.err.print(
          "Invalid usage. Expected:\n" +
          "  guestbook list <guestbook-name>\n" +
          "  guestbook greet <guestbook-name> <user> <message>\n");
      System.exit(2);
    }
  }

  /**
   * Add a greeting to the specified guestbook.
   */
  private void addGreeting(String guestbookName, String user, String message)
      throws DatastoreException {
    Entity.Builder greeting = Entity.newBuilder();
    greeting.setKey(makeKey(GUESTBOOK_KIND, guestbookName, GREETING_KIND));
    greeting.getMutableProperties().put(USER_PROPERTY, makeValue(user).build());
    greeting.getMutableProperties().put(MESSAGE_PROPERTY, makeValue(message).build());
    greeting.getMutableProperties().put(DATE_PROPERTY, makeValue(new Date()).build());
    Key greetingKey = insert(greeting.build());
    System.out.println("greeting key is: " + greetingKey);
  }

  /**
   * List the greetings in the specified guestbook.
   */
  private void listGreetings(String guestbookName) throws DatastoreException {
    Query.Builder query = Query.newBuilder();
    query.addKindBuilder().setName(GREETING_KIND);
    query.setFilter(makeFilter(KEY_PROPERTY, PropertyFilter.Operator.HAS_ANCESTOR,
        makeValue(makeKey(GUESTBOOK_KIND, guestbookName))));
    query.addOrder(makeOrder(DATE_PROPERTY, PropertyOrder.Direction.DESCENDING));

    List<Entity> greetings = runQuery(query.build());
    if (greetings.size() == 0) {
      System.out.println("no greetings in " + guestbookName);
    }
    for (Entity greeting : greetings) {
      Map<String, Value> propertyMap = greeting.getProperties();
      System.out.println(
          DatastoreHelper.toDate(propertyMap.get(DATE_PROPERTY)) + ": " +
          DatastoreHelper.getString(propertyMap.get(USER_PROPERTY)) + " says " +
          DatastoreHelper.getString(propertyMap.get(MESSAGE_PROPERTY)));
    }
  }

  /**
   * Insert an entity into the datastore.
   *
   * The entity must have no ids.
   *
   * @return The key for the inserted entity.
   * @throws DatastoreException on error
   */
  private Key insert(Entity entity) throws DatastoreException {
    CommitRequest req = CommitRequest.newBuilder()
	.addMutations(Mutation.newBuilder()
	    .setInsert(entity))
        .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
	.build();
    return datastore.commit(req).getMutationResults(0).getKey();
  }

  /**
   * Run a query on the datastore.
   *
   * @return The entities returned by the query.
   * @throws DatastoreException on error
   */
  private List<Entity> runQuery(Query query) throws DatastoreException {
    RunQueryRequest.Builder request = RunQueryRequest.newBuilder();
    request.setQuery(query);
    RunQueryResponse response = datastore.runQuery(request.build());

    if (response.getBatch().getMoreResults() == QueryResultBatch.MoreResultsType.NOT_FINISHED) {
      System.err.println("WARNING: partial results\n");
    }
    List<EntityResult> results = response.getBatch().getEntityResultsList();
    List<Entity> entities = new ArrayList<Entity>(results.size());
    for (EntityResult result : results) {
      entities.add(result.getEntity());
    }
    return entities;
  }
}
