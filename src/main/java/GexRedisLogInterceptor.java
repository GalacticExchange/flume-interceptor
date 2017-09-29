import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class GexRedisLogInterceptor implements Interceptor {

    private static Client client;
    private static Jedis jedis;

    private static String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static TimeZone timeZone = TimeZone.getTimeZone("UTC");
    private static SimpleDateFormat sdf = new SimpleDateFormat(pattern);

    private static final String PROPERTIES_FILENAME = "properties.filename";
    private String filename;
    private static String index;

    private final static String ELASTIC_IP = "elasticsearch.ip";
    private final static String ELASTIC_PORT = "elasticsearch.port";
    private final static String REDIS_IP = "redis.ip";
    private final static String REDIS_PORT = "redis.port";

    private final static String REDIS_CLUSTER = "gex:data:cluster_id_by_uid:";
    private final static String REDIS_USER = "gex:data:user_id_by_username:";
    private final static String REDIS_INSTANCE = "gex:data:instance_id_by_uid:";
    private final static String REDIS_NODE = "gex:data:node_id_by_uid:";
    private final static String REDIS_TYPE = "gex:data:type_id_by_name:";

    private final static String INDEX = "index";
    private final static String ID = "id";
    private final static String UID = "uid";
    private final static String TYPE_ID = "type_id";
    private final static String TYPE_NAME = "type_name";
    private final static String TYPE_TYPE = "log_types";
    private final static String USERNAME = "username";
    private final static String CLUSTER_TYPE = "clusters";
    private final static String USER_TYPE = "users";
    private final static String NODE_TYPE = "nodes";
    private final static String INSTANCE_TYPE = "instances";
    private final static String CLUSTER_ID = "clusterID";
    private final static String NODE_ID = "nodeID";
    private final static String INSTANCE_ID = "instanceID";
    private final static String USER_ID = "user_id";
    private final static String NEW_CLUSTER_ID = "cluster_id";
    private final static String NEW_NODE_ID = "node_id";
    private final static String NEW_INSTANCE_ID = "instance_id";
    private final static String CREATED_AT = "created_at";
    private final static String PROCESSED_AT = "processed_at";
    private final static String DATA = "data";

    private final static String NAME = "name";
    private final static String TITLE = "title";
    private final static String ENABLE = "enabled";
    private final static String VISIBLE_CLIENT = "visible_client";
    private final static String NEED_NOTIFY = "need_notify";

    private GexRedisLogInterceptor(Context context) {
        this.filename = context.getString(PROPERTIES_FILENAME);
    }

    @Override
    public void initialize() {
        Properties properties = new Properties();
        try {
            if (filename != null) {
                File file = new File(filename);
                if (file.exists() && file.length() != 0) {
                    try (InputStream input = new FileInputStream(file)) {
                        properties.load(input);
                    }
                }
            }
            client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(
                    InetAddress.getByName(properties.getProperty(ELASTIC_IP).trim()), Integer.parseInt(properties.getProperty(ELASTIC_PORT).trim())));
            jedis = new Jedis(properties.getProperty(REDIS_IP).trim(), Integer.parseInt(properties.getProperty(REDIS_PORT).trim()));
            index = properties.getProperty(INDEX);
        } catch (Throwable e) {
            throw new FlumeException("Failed to initialize interceptor.", e);
        }
    }

    @Override
    public Event intercept(Event event) {
        String result, value;
        SearchResponse response;
        try {
            JsonObject obj = new JsonParser().parse(new String(event.getBody())).getAsJsonObject();
            obj.addProperty(PROCESSED_AT, DateFormatUtils.format(System.currentTimeMillis(), pattern, timeZone));
            if (obj.has(CREATED_AT) && !obj.get(CREATED_AT).isJsonNull()) {
                String createdAt = obj.get(CREATED_AT).getAsString();
                try {
                    if (!createdAt.equals(sdf.format(sdf.parse(createdAt)))) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    JsonObject data;
                    if (obj.has(DATA) && !obj.get(DATA).isJsonNull()) {
                        data = obj.get(DATA).getAsJsonObject();
                    } else {
                        data = new JsonObject();
                    }
                    data.addProperty(CREATED_AT, createdAt);
                    obj.addProperty(DATA, data.toString());
                    obj.remove(CREATED_AT);
                }
            }
            // check type
            try {
                if (obj.has(TYPE_NAME) && !obj.get(TYPE_NAME).isJsonNull()) {
                    String type = obj.get(TYPE_NAME).getAsString();
                    response = client.prepareSearch(INDEX).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                            .setTypes(TYPE_TYPE).setQuery(QueryBuilders.matchQuery(NAME, type)).execute().actionGet();
                    if (response.getHits().getTotalHits() == 0) {
                        client.prepareIndex(INDEX, TYPE_TYPE).setSource(jsonBuilder().startObject().field(NAME, type).field(TITLE, type).
                                field(ENABLE, 1).field(VISIBLE_CLIENT, 0).field(NEED_NOTIFY, 0).endObject()).execute().actionGet();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // if event already has id fields - return this event
            if (obj.has(NEW_CLUSTER_ID) || obj.has(USER_ID) || obj.has(NEW_INSTANCE_ID) || obj.has(NEW_NODE_ID) || obj.has(TYPE_ID)) {
                return event;
            }
            // check Redis cache for if value. If not present - call Elasticsearch.
            try {
                if (obj.has(CLUSTER_ID) && !obj.get(CLUSTER_ID).isJsonNull()) {
                    value = obj.get(CLUSTER_ID).getAsString();
                    result = jedis.get(REDIS_CLUSTER + value);
                    if (result != null) {
                        obj.addProperty(NEW_CLUSTER_ID, result);
                    } else {
                        response = client.prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                                .setTypes(CLUSTER_TYPE).setQuery(QueryBuilders.matchQuery(UID, value)).execute().actionGet();
                        result = response.getHits().getAt(0).getSource().get(ID).toString();
                        obj.addProperty(NEW_CLUSTER_ID, result);
                        jedis.set(REDIS_CLUSTER + value, result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (obj.has(USERNAME) && !obj.get(USERNAME).isJsonNull()) {
                    value = obj.get(USERNAME).getAsString();
                    result = jedis.get(REDIS_USER + value);
                    if (result != null) {
                        obj.addProperty(USER_ID, result);
                    } else {
                        response = client.prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                                .setTypes(USER_TYPE).setQuery(QueryBuilders.matchQuery(USERNAME, value)).execute().actionGet();
                        result = response.getHits().getAt(0).getSource().get(ID).toString();
                        obj.addProperty(USER_ID, result);
                        jedis.set(REDIS_USER + value, result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (obj.has(NODE_ID) && !obj.get(NODE_ID).isJsonNull()) {
                    value = obj.get(NODE_ID).getAsString();
                    result = jedis.get(REDIS_NODE + value);
                    if (result != null) {
                        obj.addProperty(NEW_NODE_ID, result);
                    } else {
                        response = client.prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                                .setTypes(NODE_TYPE).setQuery(QueryBuilders.matchQuery(UID, value)).execute().actionGet();
                        result = response.getHits().getAt(0).getSource().get(ID).toString();
                        obj.addProperty(NEW_NODE_ID, result);
                        jedis.set(REDIS_NODE + value, result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (obj.has(INSTANCE_ID) && !obj.get(INSTANCE_ID).isJsonNull()) {
                    value = obj.get(INSTANCE_ID).getAsString();
                    result = jedis.get(REDIS_INSTANCE + value);
                    if (result != null) {
                        obj.addProperty(NEW_INSTANCE_ID, result);
                    } else {
                        response = client.prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                                .setTypes(INSTANCE_TYPE).setQuery(QueryBuilders.matchQuery(UID, value)).execute().actionGet();
                        result = response.getHits().getAt(0).getSource().get(ID).toString();
                        obj.addProperty(NEW_INSTANCE_ID, result);
                        jedis.set(REDIS_INSTANCE + value, result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (obj.has(TYPE_NAME) && !obj.get(TYPE_NAME).isJsonNull()) {
                    value = obj.get(TYPE_NAME).getAsString();
                    result = jedis.get(REDIS_TYPE + value);
                    if (result != null) {
                        obj.addProperty(TYPE_ID, result);
                    } else {
                        response = client.prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                                .setTypes(TYPE_TYPE).setQuery(QueryBuilders.matchQuery(NAME, value)).execute().actionGet();
                        result = response.getHits().getAt(0).getSource().get(ID).toString();
                        obj.addProperty(TYPE_ID, result);
                        jedis.set(REDIS_TYPE + value, result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return EventBuilder.withBody(obj.toString().getBytes(), event.getHeaders());
        } catch (Throwable e) {
            e.printStackTrace();
            return event;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        LinkedList<Event> intercepted = new LinkedList<>();

        for (Event event : events) {
            try {
                intercepted.add(intercept(event));
            } catch (Throwable e) {
                //accept only formatted events
            }
        }

        return intercepted;
    }

    @Override
    public void close() {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Builder implements Interceptor.Builder {
        private Context ctx;

        @Override
        public void configure(Context context) {
            this.ctx = context;
        }

        @Override
        public Interceptor build() {
            return new GexRedisLogInterceptor(ctx);
        }
    }
}

