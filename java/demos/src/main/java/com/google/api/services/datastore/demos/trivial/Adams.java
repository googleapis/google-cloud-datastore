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
package com.google.api.services.datastore.demos.trivial;

import com.google.api.services.datastore.DatastoreV1.*;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * A trivial command-line application using the Datastore API.
 */
public class Adams {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: Adams <DATASET_ID>");
      System.exit(1);
    }
    // Set the dataset from the command line parameters.
    String datasetId = args[0];
    Datastore datastore = null;
    try {
      // Setup the connection to Google Cloud Datastore and infer credentials
      // from the environment.
      datastore = DatastoreFactory.get().create(DatastoreHelper.getOptionsfromEnv()
          .dataset(datasetId).build());
    } catch (GeneralSecurityException exception) {
      System.err.println("Security error connecting to the datastore: " + exception.getMessage());
      System.exit(1);
    } catch (IOException exception) {
      System.err.println("I/O error connecting to the datastore: " + exception.getMessage());
      System.exit(1);
    }

    try {
      // Create an RPC request to write mutations outside of a transaction.
      BlindWriteRequest.Builder req = BlindWriteRequest.newBuilder();
      // Create a new entity.
      Entity.Builder entity = Entity.newBuilder();
      // Set the entity key with only one `path_element`: no parent.
      Key.Builder key = Key.newBuilder().addPathElement(
          Key.PathElement.newBuilder()
          .setKind("Trivia")
          .setName("hgtg"));
      entity.setKey(key);
      // Add two entity properties:
      // - a utf-8 string: `question`
      entity.addProperty(Property.newBuilder()
          .setName("question")
          .addValue(Value.newBuilder()
              .setStringValue("Meaning of Life?")));
      // - a 64bit integer: `answer`
      entity.addProperty(Property.newBuilder()
          .setName("answer")
          .addValue(Value.newBuilder()
              .setIntegerValue(42)));
      // Add mutation to the request that update or insert this entity.
      req.getMutationBuilder().addUpsert(entity);
      // Execute the RPC synchronously and ignore the response.
      datastore.blindWrite(req.build());
      // Create an RPC request to get entities by key.
      LookupRequest.Builder lreq = LookupRequest.newBuilder();
      // Add one key to lookup the same entity.
      lreq.addKey(key);
      // Execute the RPC and get the response.
      LookupResponse lresp = datastore.lookup(lreq.build());
      // Found one entity result.
      Entity entityFound = lresp.getFound(0).getEntity();
      // Get `question` property value.
      String question = entityFound.getProperty(0).getValue(0).getStringValue();
      // Get `answer` property value.
      Long answer = entityFound.getProperty(1).getValue(0).getIntegerValue();
      System.out.println(question);
      String result = System.console().readLine("> ");
      if (result.equals(answer.toString())) {
        System.out.println("fascinating, extraordinary and," +
            "when you think hard about it, completely obvious.");
      } else {
        System.out.println("Don't Panic!");
      }
    } catch (DatastoreException exception) {
      // Catch all Datastore rpc errors.
      System.err.println("Error while doing datastore operation");
      // Log the exception, the name of the method called and the error code.
      System.err.println(String.format("DatastoreException(%s): %s %s",
              exception.getMessage(),
              exception.methodName,
              exception.code));
      System.exit(1);
    }
  }
}
