package org.opensearch.rest.action.service;

import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.action.model.Person;
import org.opensearch.rest.action.utils.Routing;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class PersonService {

    private static final int QSIZE = 9999;

    public static void createPerson(Person person,
                                    WriteRequest.RefreshPolicy refreshPolicy,
                                    Client client,
                                    ActionListener<IndexResponse> listener) throws IOException {

        IndexRequest indexRequest = new IndexRequest()
                .index(Routing.PERSON_INDEX_NAME)
                .id(person.getId())
                .source(person.toXContent(JsonXContent.contentBuilder(), null))
                .setRefreshPolicy(refreshPolicy);

        client.index(indexRequest, listener);
    }

    public static void getPersons(Client client, ActionListener<SearchResponse> listener) {

        SearchRequest personSearchRequest = new SearchRequest()
                .indices(Routing.PERSON_INDEX_NAME)
                .source(new SearchSourceBuilder()
                        .seqNoAndPrimaryTerm(true)
                        .query(QueryBuilders.matchAllQuery())
                        .size(QSIZE));

        client.search(personSearchRequest, listener);
    }

    public static void getPerson(String id,
                                 Client client,
                                 ActionListener<SearchResponse> listener) {

        SearchRequest personSearchRequest = new SearchRequest()
                .indices(Routing.PERSON_INDEX_NAME)
                .source(new SearchSourceBuilder()
                        .seqNoAndPrimaryTerm(true)
                        .size(1)
                        .query(QueryBuilders.idsQuery().addIds(id)));

        client.search(personSearchRequest, listener);
    }

    public static void putPerson(String id,
                                 Person person,
                                 WriteRequest.RefreshPolicy refreshPolicy,
                                 Client client,
                                 ActionListener<IndexResponse> listener) throws IOException {

        IndexRequest indexRequest = new IndexRequest()
                .index(Routing.PERSON_INDEX_NAME)
                .id(id)
                .source(person.toXContent(JsonXContent.contentBuilder(), null))
                .setRefreshPolicy(refreshPolicy);

        client.index(indexRequest, listener);
    }

    public static void deletePerson(String id,
                                    WriteRequest.RefreshPolicy refreshPolicy,
                                    Client client,
                                    ActionListener<DeleteResponse> listener) {

        DeleteRequest deleteRequest = new DeleteRequest()
                .index(Routing.PERSON_INDEX_NAME)
                .id(id).setRefreshPolicy(refreshPolicy);

        client.delete(deleteRequest, listener);
    }

    public static String avg(SearchResponse searchResponse, String fieldName) {
        if (searchResponse.getHits().getTotalHits().value == 0) {
            throw new IllegalArgumentException("Empty person list provided.");
        }

        OptionalDouble result = Arrays.stream(searchResponse.getHits().getHits())
                .filter(e -> e.getSourceAsMap().getOrDefault(fieldName, null) instanceof Number)
                .mapToInt(e -> ((Number)e.getSourceAsMap().get(fieldName)).intValue())
                .average();

        if (result.isEmpty()) {
            throw new IllegalArgumentException("Invalid person list provided (all values are not numbers).");
        }

        return String.valueOf(result.getAsDouble());
    }

    public static String max(SearchResponse searchResponse, String fieldName) {
        if (searchResponse.getHits().getTotalHits().value == 0) {
            throw new IllegalArgumentException("Empty person list provided.");
        }

        OptionalInt result = Arrays.stream(searchResponse.getHits().getHits())
                .filter(e -> e.getSourceAsMap().getOrDefault(fieldName, null) instanceof Number)
                .mapToInt(e -> ((Number) e.getSourceAsMap().get(fieldName)).intValue())
                .max();

        if (result.isEmpty()) {
            throw new IllegalArgumentException("Invalid person list provided (all values are not numbers).");
        }

        return String.valueOf(result.getAsInt());
    }

    public static String values(SearchResponse searchResponse, String fieldName) {
        Set<Object> uniqueValues = Arrays.stream(searchResponse.getHits().getHits())
                .filter(e -> e.getSourceAsMap().getOrDefault(fieldName, null) != null)
                .map(e -> e.getSourceAsMap().get(fieldName))
                .collect(Collectors.toCollection(HashSet::new));

        StringBuilder result = new StringBuilder();
        result.append("[");
        for (var v : uniqueValues) { result.append("\"" + v.toString() + "\","); }
        result.deleteCharAt(result.toString().length()-1);
        result.append("]");

        return result.toString();
    }
}