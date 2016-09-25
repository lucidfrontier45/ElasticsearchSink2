/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.frontier45.flume.sink.elasticsearch2;

import com.frontier45.flume.sink.elasticsearch2.client.ElasticSearchClient;
import com.frontier45.flume.sink.elasticsearch2.client.ElasticSearchClientFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A sink which reads events from a channel and writes them to ElasticSearch
 * based on the work done by https://github.com/Aconex/elasticflume.git.</p>
 * 
 * This sink supports batch reading of events from the channel and writing them
 * to ElasticSearch.</p>
 * 
 * Indexes will be rolled daily using the format 'indexname-YYYY-MM-dd' to allow
 * easier management of the index</p>
 * 
 * This sink must be configured with with mandatory parameters detailed in
 * {@link ElasticSearchSinkConstants}</p> It is recommended as a secondary step
 * the ElasticSearch indexes are optimized for the specified serializer. This is
 * not handled by the sink but is typically done by deploying a config template
 * alongside the ElasticSearch deploy</p>
 * 
 * @see http
 *      ://www.elasticsearch.org/guide/reference/api/admin-indices-templates.
 *      html
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchSink.class);
  public static final String FAILED_TO_COMMIT_TRANSACTION_TRANSACTION_ROLLED_BACK = "Failed to commit transaction. Transaction rolled back.";
  public static final String MISSING_PARAM = "Missing Param:";

  // Used for testing
  private boolean isLocal = false;
  private final CounterGroup counterGroup = new CounterGroup();

  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;
  private long ttlMs = ElasticSearchSinkConstants.DEFAULT_TTL;
  private String clusterName = ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;
  private String indexName = ElasticSearchSinkConstants.DEFAULT_INDEX_NAME;
  private String indexType = ElasticSearchSinkConstants.DEFAULT_INDEX_TYPE;
  private String clientType = ElasticSearchSinkConstants.DEFAULT_CLIENT_TYPE;
  private final Pattern pattern = Pattern.compile(ElasticSearchSinkConstants.TTL_REGEX,
      Pattern.CASE_INSENSITIVE);
  private Matcher matcher = pattern.matcher("");

  private String[] serverAddresses = null;

  private ElasticSearchClient client = null;
  private Context elasticSearchClientContext = null;

  private ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
  private ElasticSearchEventSerializer eventSerializer;
  private IndexNameBuilder indexNameBuilder;
  private SinkCounter sinkCounter;

  /**
   * Create an {@link ElasticSearchSink} configured using the supplied
   * configuration
   */
  public ElasticSearchSink() {
    this(false);
  }

  /**
   * Create an {@link ElasticSearchSink}</p>
   * 
   * @param isLocal
   *          If <tt>true</tt> sink will be configured to only talk to an
   *          ElasticSearch instance hosted in the same JVM, should always be
   *          false is production
   * 
   */
  @VisibleForTesting
  ElasticSearchSink(boolean isLocal) {
    this.isLocal = isLocal;
  }

  @VisibleForTesting
  String[] getServerAddresses() {
    return serverAddresses;
  }

  @VisibleForTesting
  String getClusterName() {
    return clusterName;
  }

  @VisibleForTesting
  String getIndexName() {
    return indexName;
  }

  @VisibleForTesting
  String getIndexType() {
    return indexType;
  }

  @VisibleForTesting
  long getTTLMs() {
    return ttlMs;
  }

  @VisibleForTesting
  ElasticSearchEventSerializer getEventSerializer() {
    return eventSerializer;
  }

  @VisibleForTesting
  IndexNameBuilder getIndexNameBuilder() {
    return indexNameBuilder;
  }

  @Override
  public Status process() throws EventDeliveryException {
    logger.debug("processing...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      int count;
      for (count = 0; count < batchSize; ++count) {
        Event event = channel.take();

        if (event == null) {
          break;
        }
        String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());
        client.addEvent(event, indexNameBuilder, realIndexType, ttlMs);
      }

      if (count <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;
      } else {
        if (count < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(count);
        client.execute();
      }
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(count);
      counterGroup.incrementAndGet("transaction.success");
    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        logger.error(
            "Exception in rollback. Rollback might not have been successful.",
            ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        logger.error(FAILED_TO_COMMIT_TRANSACTION_TRANSACTION_ROLLED_BACK,
            ex);
        Throwables.propagate(ex);
      } else {
        logger.error(FAILED_TO_COMMIT_TRANSACTION_TRANSACTION_ROLLED_BACK,
            ex);
        throw new EventDeliveryException(
                FAILED_TO_COMMIT_TRANSACTION_TRANSACTION_ROLLED_BACK, ex);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    if (!isLocal) {
      if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.HOSTNAMES))) {
        serverAddresses = StringUtils.deleteWhitespace(
            context.getString(ElasticSearchSinkConstants.HOSTNAMES)).split(",");
      }
      Preconditions.checkState(serverAddresses != null
          && serverAddresses.length > 0, MISSING_PARAM + ElasticSearchSinkConstants.HOSTNAMES);
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_NAME))) {
      this.indexName = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_TYPE))) {
      this.indexType = context.getString(ElasticSearchSinkConstants.INDEX_TYPE);
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.CLUSTER_NAME))) {
      this.clusterName = context.getString(ElasticSearchSinkConstants.CLUSTER_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.BATCH_SIZE))) {
      this.batchSize = Integer.parseInt(context.getString(ElasticSearchSinkConstants.BATCH_SIZE));
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.TTL))) {
      this.ttlMs = parseTTL(context.getString(ElasticSearchSinkConstants.TTL));
      Preconditions.checkState(ttlMs > 0, ElasticSearchSinkConstants.TTL
          + " must be greater than 0 or not set.");
    }

    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.CLIENT_TYPE))) {
      clientType = context.getString(ElasticSearchSinkConstants.CLIENT_TYPE);
    }

    elasticSearchClientContext = new Context();
    elasticSearchClientContext.putAll(context.getSubProperties(ElasticSearchSinkConstants.CLIENT_PREFIX));

    String serializerClazz = ElasticSearchSinkConstants.DEFAULT_SERIALIZER_CLASS;
    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.SERIALIZER))) {
      serializerClazz = context.getString(ElasticSearchSinkConstants.SERIALIZER);
    }

    Context serializerContext = new Context();
    serializerContext.putAll(context.getSubProperties(ElasticSearchSinkConstants.SERIALIZER_PREFIX));

    try {
      @SuppressWarnings("unchecked")
      Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
          .forName(serializerClazz);
      Configurable serializer = clazz.newInstance();

      if (serializer instanceof ElasticSearchIndexRequestBuilderFactory) {
        indexRequestFactory
            = (ElasticSearchIndexRequestBuilderFactory) serializer;
        indexRequestFactory.configure(serializerContext);
      } else if (serializer instanceof ElasticSearchEventSerializer) {
        eventSerializer = (ElasticSearchEventSerializer) serializer;
        eventSerializer.configure(serializerContext);
      } else {
        throw new IllegalArgumentException(serializerClazz
            + " is not an ElasticSearchEventSerializer");
      }
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    String indexNameBuilderClass = ElasticSearchSinkConstants.DEFAULT_INDEX_NAME_BUILDER_CLASS;
    if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_NAME_BUILDER))) {
      indexNameBuilderClass = context.getString(ElasticSearchSinkConstants.INDEX_NAME_BUILDER);
    }

    Context indexnameBuilderContext = new Context();
    serializerContext.putAll(
            context.getSubProperties(ElasticSearchSinkConstants.INDEX_NAME_BUILDER_PREFIX));

    try {
      @SuppressWarnings("unchecked")
      Class<? extends IndexNameBuilder> clazz
              = (Class<? extends IndexNameBuilder>) Class
              .forName(indexNameBuilderClass);
      indexNameBuilder = clazz.newInstance();
      indexnameBuilderContext.put(ElasticSearchSinkConstants.INDEX_NAME, indexName);
      indexNameBuilder.configure(indexnameBuilderContext);
    } catch (Exception e) {
      logger.error("Could not instantiate index name builder.", e);
      Throwables.propagate(e);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    Preconditions.checkState(StringUtils.isNotBlank(indexName),
        MISSING_PARAM + ElasticSearchSinkConstants.INDEX_NAME);
    Preconditions.checkState(StringUtils.isNotBlank(indexType),
        MISSING_PARAM + ElasticSearchSinkConstants.INDEX_TYPE);
    Preconditions.checkState(StringUtils.isNotBlank(clusterName),
        MISSING_PARAM + ElasticSearchSinkConstants.CLUSTER_NAME);
    Preconditions.checkState(batchSize >= 1, ElasticSearchSinkConstants.BATCH_SIZE
        + " must be greater than 0");
  }

  @Override
  public void start() {
    ElasticSearchClientFactory clientFactory = new ElasticSearchClientFactory();

    logger.info("ElasticSearch sink {} started");
    sinkCounter.start();
    try {
      if (isLocal) {
        client = clientFactory.getLocalClient(
            clientType, eventSerializer, indexRequestFactory);
      } else {
        client = clientFactory.getClient(clientType, serverAddresses,
            clusterName, eventSerializer, indexRequestFactory);
        client.configure(elasticSearchClientContext);
      }
      sinkCounter.incrementConnectionCreatedCount();
    } catch (Exception ex) {
      ex.printStackTrace();
      sinkCounter.incrementConnectionFailedCount();
      if (client != null) {
        client.close();
        sinkCounter.incrementConnectionClosedCount();
      }
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("ElasticSearch sink {} stopping");
    if (client != null) {
      client.close();
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
    super.stop();
  }

  /*
   * Returns TTL value of ElasticSearch index in milliseconds when TTL specifier
   * is "ms" / "s" / "m" / "h" / "d" / "w". In case of unknown specifier TTL is
   * not set. When specifier is not provided it defaults to days in milliseconds
   * where the number of days is parsed integer from TTL string provided by
   * user. <p> Elasticsearch supports ttl values being provided in the format:
   * 1d / 1w / 1ms / 1s / 1h / 1m specify a time unit like d (days), m
   * (minutes), h (hours), ms (milliseconds) or w (weeks), milliseconds is used
   * as default unit.
   * http://www.elasticsearch.org/guide/reference/mapping/ttl-field/.
   * 
   * @param ttl TTL value provided by user in flume configuration file for the
   * sink
   * 
   * @return the ttl value in milliseconds
   */
  private long parseTTL(String ttl) {
    matcher = matcher.reset(ttl);
    while (matcher.find()) {
      if (matcher.group(2).equals("ms")) {
        return Long.parseLong(matcher.group(1));
      } else if (matcher.group(2).equals("s")) {
        return TimeUnit.SECONDS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("m")) {
        return TimeUnit.MINUTES.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("h")) {
        return TimeUnit.HOURS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("d")) {
        return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("w")) {
        return TimeUnit.DAYS.toMillis(7L * Integer.parseInt(matcher.group(1)));
      } else if (matcher.group(2).equals("")) {
        logger.info("TTL qualifier is empty. Defaulting to day qualifier.");
        return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
      } else {
        logger.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
        return 0;
      }
    }
    logger.info("TTL not provided. Skipping the TTL config by returning 0.");
    return 0;
  }
}
