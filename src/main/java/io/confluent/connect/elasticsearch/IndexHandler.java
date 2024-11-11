/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class IndexHandler {
  private static final Logger log = LoggerFactory.getLogger(IndexHandler.class);

  private final ElasticsearchClient client;
  private final ElasticsearchSinkConnectorConfig config;
  private final Set<String> existingMappings;
  private final Set<String> indexCache;

  private final ObjectMapper objectMapper;


  IndexHandler(ElasticsearchSinkConnectorConfig config, ElasticsearchClient client) {
    this.client = client;
    this.config = config;
    this.existingMappings = new HashSet<>();
    this.indexCache = new HashSet<>();
    objectMapper = new ObjectMapper();
  }

  void checkMapping(String index, SinkRecord record) {
    if (!config.shouldIgnoreSchema(record.topic()) && !existingMappings.contains(index)) {
      if (!client.hasMapping(index)) {
        client.createMapping(index, record.valueSchema());
      }
      log.debug("Caching mapping for index '{}' locally.", index);
      existingMappings.add(index);
    }
  }

  /**
   * Returns the converted index name from a given topic name. Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  private String convertTopicToIndexName(String topic) {
    String index = topic.toLowerCase();
    if (index.length() > 255) {
      index = index.substring(0, 255);
    }

    if (index.startsWith("-") || index.startsWith("_")) {
      index = index.substring(1);
    }

    if (index.equals(".") || index.equals("..")) {
      index = index.replace(".", "dot");
      log.warn("Elasticsearch cannot have indices named {}. Index will be named {}.", topic, index);
    }

    if (!topic.equals(index)) {
      log.trace("Topic '{}' was translated to index '{}'.", topic, index);
    }

    return index;
  }

  /**
   * Returns the converted datastream name from a given topic name in the form:
   * {type}-{dataset}-{namespace}
   * For the <code>namespace</code> (that can contain topic), Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>no longer than 100 bytes</li>
   * </ul>
   * (<a href="https://github.com/elastic/ecs/blob/master/rfcs/text/0009-data_stream-fields.md#restrictions-on-values">ref</a>_.)
   */
  private String convertTopicToDataStreamName(String topic) {
    String namespace = config.dataStreamNamespace();
    namespace = namespace.replace("${topic}", topic.toLowerCase());
    if (namespace.length() > 100) {
      namespace = namespace.substring(0, 100);
    }
    String dataStream = String.format(
        "%s-%s-%s",
        config.dataStreamType().toLowerCase(),
        config.dataStreamDataset(),
        namespace
    );
    return dataStream;
  }

  /**
   * Returns the converted index name from a given topic name. If writing to a data stream,
   * returns the index name in the form {type}-{dataset}-{topic}. For both cases, Elasticsearch
   * accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  String createIndexName(String topic) {
    return config.isDataStream()
        ? convertTopicToDataStreamName(topic)
        : convertTopicToIndexName(topic);
  }

  void ensureIndexExists(String index) {
    if (!indexCache.contains(index)) {
      log.info("Creating index {}.", index);
      client.createIndexOrDataStream(index);
      indexCache.add(index);
    }
  }

  private String getDataStreamFromPayload(String payload) {
    String dataStream;
    try {
      JsonNode jsonNode = objectMapper.readTree(payload);
      if (!jsonNode.isObject()) {
        throw new DataException("Top level payload contains data of Json type "
            + jsonNode.getNodeType() + ". Required Json object.");
      }
      if (jsonNode.get(config.dataStreamKey()).isEmpty()) {
        return null;
      }
      dataStream = String.format(
          "%s-%s-%s",
          String.valueOf(jsonNode.get(config.dataStreamKey()).get("type")).toLowerCase(),
          String.valueOf(jsonNode.get(config.dataStreamKey()).get("dataset")).toLowerCase(),
          String.valueOf(jsonNode.get(config.dataStreamKey()).get("namespace")).toLowerCase()
      );
    } catch (JsonProcessingException e) {
      return null;
    }
    return dataStream;
  }

  String createDataStreamName(String payload) {
    JsonNode jsonNode = null;
    try {
      jsonNode = objectMapper.readTree(payload);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    if (!jsonNode.isObject()) {
      throw new DataException("Top level payload contains data of Json type "
          + jsonNode.getNodeType() + ". Required Json object.");
    }

    JsonNode dataStreamObject = jsonNode.get(config.dataStreamKey());
    return String.format(
        "%s-%s-%s",
        dataStreamObject.get("type").asText().toLowerCase(),
        dataStreamObject.get("dataset").asText().toLowerCase(),
        dataStreamObject.get("namespace").asText().toLowerCase());
  }

}
