package org.opensearch.rest.action.resthandler;

import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.rest.*;
import org.opensearch.rest.action.model.Person;
import org.opensearch.rest.action.service.PersonService;
import org.opensearch.rest.action.utils.Routing;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.threadpool.ThreadPool.Names.REFRESH;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class PersonIndexRestHandler extends MyRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new RestHandler.Route(DELETE, Routing.ID_PERSON_URI),
                new RestHandler.Route(GET, Routing.ID_PERSON_URI),
                new RestHandler.Route(PUT, Routing.ID_PERSON_URI)));
    }

    @Override
    public String getName() {
        return "Person index handler";
    }

    @Override
    protected void initRequest(RestChannel restChannel, RestRequest request, NodeClient client) throws Exception {
        switch (request.method()) {
            case GET:
                getPerson(restChannel, request, client);
                break;
            case PUT:
                putPerson(restChannel, request, client);
                break;
            case DELETE:
                deletePerson(restChannel, request, client);
                break;
            default:
                restChannel.sendResponse(
                        new BytesRestResponse(
                                RestStatus.METHOD_NOT_ALLOWED, request.method() + " is not allowed."));
        }
    }

    private void deletePerson(RestChannel restChannel, RestRequest request, NodeClient client) {

        String id = request.param("id");

        if (id == null) {
            throw new IllegalArgumentException("Must specify id.");
        }

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH,
                WriteRequest.RefreshPolicy.IMMEDIATE.getValue()));

        PersonService.deletePerson(id, refreshPolicy, client, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.OK, "Person deleted."));
            }

            @Override
            public void onFailure(Exception e) {
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
            }
        });
    }

    private void getPerson(RestChannel restChannel, RestRequest request, NodeClient client) {

        String id = request.param("id");

        if (id == null) {
            throw new IllegalArgumentException("Must specify id.");
        }

        PersonService.getPerson(id, client, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                SearchHit hit = searchResponse.getHits().getAt(0);
                String person = hit.getSourceAsString();

                restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, person));
            }

            @Override
            public void onFailure(Exception e) {
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
            }
        });
    }

    private void putPerson(RestChannel restChannel, RestRequest request, NodeClient client) throws IOException {

        String id = request.param("id");
        XContentParser xcp = request.contentParser();
        XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp);
        Person person = Person.parse(xcp, id);

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH,
                WriteRequest.RefreshPolicy.IMMEDIATE.getValue()));

        PersonService.putPerson(id, person, refreshPolicy, client, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                try {
                    RestResponse restResponse = new BytesRestResponse(RestStatus.OK,
                            indexResponse.toXContent(JsonXContent.contentBuilder(), null));
                    restChannel.sendResponse(restResponse);
                } catch (IOException e) {
                    restChannel.sendResponse(
                            new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                }
            }

            @Override
            public void onFailure(Exception e) {
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
            }
        });
    }
}
