package org.opensearch.rest.action.resthandler;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import java.util.Map;

public abstract class MyRestHandler extends BaseRestHandler {

    private void consumeAllParamsWorkaround(RestRequest request) {
        Map<String, String> params = request.params();
        params.forEach((k, v) -> request.param(k));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        consumeAllParamsWorkaround(request);
        return channel -> {
            try {
                initRequest(channel, request, client);
            } catch (final Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    protected abstract void initRequest(RestChannel restChannel, RestRequest request, NodeClient client) throws Exception;
}
