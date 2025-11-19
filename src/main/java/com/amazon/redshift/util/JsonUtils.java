package com.amazon.redshift.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON parsing utilities for authentication providers.
 */
public class JsonUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Parses a JSON string into a JsonNode object.
   *
   * @param json The JSON string to parse. Must not be null.
   * @return A JsonNode representing the parsed JSON structure.
   * @throws JsonProcessingException if the input is not valid JSON content
   *         or if there are any other problems parsing the JSON content.
   * @throws IllegalArgumentException if the input string is null.
   */
  public static JsonNode parseJson(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readTree(json);
  }
}
