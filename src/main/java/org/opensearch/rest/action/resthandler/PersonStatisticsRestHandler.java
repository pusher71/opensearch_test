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

import static org.opensearch.rest.RestRequest.Method.*;
import static org.opensearch.threadpool.ThreadPool.Names.REFRESH;

public class PersonStatisticsRestHandler extends MyRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(new RestHandler.Route(GET, Routing.STATS_PERSON_URI)));
    }

    @Override
    public String getName() {
        return "Person statistics handler";
    }

    @Override
    protected void initRequest(RestChannel restChannel, RestRequest request, NodeClient client) throws Exception {
        switch (request.method()) {
            case GET:
                calcStatsOnPersons(restChannel, request, client);
                break;
            default:
                restChannel.sendResponse(
                        new BytesRestResponse(
                                RestStatus.METHOD_NOT_ALLOWED, request.method() + " is not allowed."));
        }
    }

    private void calcStatsOnPersons(RestChannel restChannel, RestRequest request, NodeClient client) {
        String funcType = request.param("func");
        String fieldName = request.param("field");

        if (funcType == null) {
            throw new IllegalArgumentException("Must specify function type: avg, max, values.");
        }
        else if (!funcType.equals("avg") && !funcType.equals("max") && !funcType.equals("values")) {
            throw new IllegalArgumentException("Illegal function type, must be on the following: avg, max, values.");
        }
        else if (fieldName == null) {
            throw new IllegalArgumentException("Must specify field name.");
        }

        PersonService.getPersons(client, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.getHits().getTotalHits().value > 0) {
                    String result = "{ unknown: \"true\" }";
                    switch (funcType) {
                        case "avg"   : result = PersonService.avg(searchResponse, fieldName); break;
                        case "max"   : result = PersonService.max(searchResponse, fieldName); break;
                        case "values": result = PersonService.values(searchResponse, fieldName); break;
                    }

                    restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, result));
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
}
