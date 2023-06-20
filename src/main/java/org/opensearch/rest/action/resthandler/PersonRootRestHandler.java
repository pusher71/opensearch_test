package org.opensearch.rest.action.resthandler;

import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.UUIDs;
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

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.threadpool.ThreadPool.Names.REFRESH;

public class PersonRootRestHandler extends MyRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(GET, Routing.ROOT_PERSON_URI),
                new Route(POST, Routing.ROOT_PERSON_URI)));
    }

    @Override
    public String getName() {
        return "Person root handler";
    }

    @Override
    protected void initRequest(RestChannel restChannel, RestRequest request, NodeClient client) throws Exception {
        switch (request.method()) {
            case POST:
                createPerson(restChannel, request, client);
                break;
            case GET:
                getPersons(restChannel, request, client);
                break;
            default:
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, request.method()
                                + " is not allowed."));
        }
    }

    private void createPerson(RestChannel restChannel, RestRequest request, NodeClient client) throws IOException {

        String id = generateUUID();
        XContentParser xcp = request.contentParser();
        XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp);
        Person person = Person.parse(xcp, id);

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH,
                WriteRequest.RefreshPolicy.IMMEDIATE.getValue()));

        PersonService.createPerson(person, refreshPolicy, client, new ActionListener<IndexResponse>() {
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

    private void getPersons(RestChannel restChannel, RestRequest request, NodeClient client) {
        PersonService.getPersons(client, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                StringBuilder sb = new StringBuilder();

                if (searchResponse.getHits().getTotalHits().value > 0) {

                    sb.append("[");

                    for (SearchHit hit : searchResponse.getHits()) {
                        sb.append(hit.toString());
                        sb.append(",");
                    }

                    sb.deleteCharAt(sb.toString().length() - 1);

                    sb.append("]");
                    restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, sb.toString()));
                } else {
                    restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, "[]"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                restChannel.sendResponse(
                        new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
            }
        });
    }

    public static String generateUUID() {
        return UUIDs.base64UUID();
    }
}
