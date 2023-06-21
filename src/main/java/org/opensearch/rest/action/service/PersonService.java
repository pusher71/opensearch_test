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
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public final class PersonService {

    private static final int QSIZE = 9999;

    private PersonService() {
    }

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
}
