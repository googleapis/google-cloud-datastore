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
package com.google.api.services.datastore.client;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;

import java.util.Arrays;
import java.util.List;

/**
 * An immutable object containing settings for the datastore.
 *
 * <p>Example for connecting to a datastore:</p>
 *
 * <pre>
 * DatastoreOptions options = new DatastoreOptions.Builder()
 *     .dataset("my-dataset-id")
 *     .credential(DatastoreHelper.getComputeEngineCredential())
 *     .build();
 * DatastoreFactory.get().create(options);
 * </pre>
 *
 * <p>
 * The options should be passed to {@link DatastoreFactory#create}.
 * </p>
 *
 */
public class DatastoreOptions {
  private final String dataset;
  private final String host;
  private static final String DEFAULT_HOST = "https://www.googleapis.com";

  private final HttpRequestInitializer initializer;

  private final Credential credential;
  private final HttpTransport transport;
  public static final List<String> SCOPES = Arrays.asList(
      "https://www.googleapis.com/auth/datastore",
      "https://www.googleapis.com/auth/userinfo.email");

  DatastoreOptions(Builder b) {
    this.dataset = b.dataset;
    this.host = b.host != null ? b.host : DEFAULT_HOST;
    this.initializer = b.initializer;
    this.credential = b.credential;
    this.transport = b.transport;
  }

  /**
   * Builder for {@link DatastoreOptions}.
   */
  public static class Builder {
    private String dataset;
    private String host;
    private HttpRequestInitializer initializer;
    private Credential credential;
    private HttpTransport transport;

    public Builder() { }

    public Builder(DatastoreOptions options) {
      this.dataset = options.dataset;
      this.host = options.host;
      this.initializer = options.initializer;
      this.credential = options.credential;
      this.transport = options.transport;
    }

    public DatastoreOptions build() {
      return new DatastoreOptions(this);
    }

    /**
     * Sets the dataset used to access the datastore.
     */
    public Builder dataset(String newDataset) {
      dataset = newDataset;
      return this;
    }

    /**
     * Sets the host used to access the datastore.
     */
    public Builder host(String newHost) {
      host = newHost;
      return this;
    }

    /**
     * Sets the (optional) initializer to run on HTTP requests to the API.
     */
    public Builder initializer(HttpRequestInitializer newInitializer) {
      initializer = newInitializer;
      return this;
    }

    /**
     * Sets the Google APIs credentials used to access the API.
     */
    public Builder credential(Credential newCredential) {
      credential = newCredential;
      return this;
    }
    
    /**
     * Sets the transport used to access the API.
     */
    public Builder transport(HttpTransport transport) {
      this.transport = transport;
      return this;
    }
  }

  // === getters ===

  public String getDataset() {
    return dataset;
  }

  public String getHost() {
    return host;
  }

  public HttpRequestInitializer getInitializer() {
    return initializer;
  }

  public Credential getCredential() {
    return credential;
  }
  
  public HttpTransport getTransport() {
    return transport;
  }
}