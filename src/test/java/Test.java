import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import redis.clients.jedis.Jedis;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class Test {
    private static String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static TimeZone timeZone = TimeZone.getTimeZone("UTC");
    private static SimpleDateFormat sdf = new SimpleDateFormat(pattern);

    private final static String IP = "51.0.0.63";
    private final static int PORT = 9300;
    private final static String INDEX = "gex.models";
    private final static String ID = "id";
    private final static String TYPE = "log_types";
    private final static String REDIS_USER = "gex:data:user_id_by_username:";
    private final static String USERNAME = "username";
    private final static String USER_TYPE = "users";
    public static void main(String[] arguments) {
        String createdAt = "2017-05-10T09:36:30.612Z";//"2017-05-10 07:11:56 UTC";
        try {
            if (!createdAt.equals(sdf.format(sdf.parse(createdAt)))) {
                throw new Exception();
            }
        } catch (Exception e) {
        System.out.println("HELLO");
        }
        //String name = "kafka";
        Client client = null;
        SearchResponse response;
        try {
            client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));

            Jedis jedis = new Jedis("51.0.0.65", 6379);
            String value = "jeff-jakubowski";
            String result = jedis.get(REDIS_USER + value);
            if (result != null) {
                System.out.println("PRESENT");
            } else {
                response = client.prepareSearch(INDEX).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                        .setTypes(USER_TYPE).setQuery(QueryBuilders.matchQuery(USERNAME, value)).execute().actionGet();
                result = response.getHits().getAt(0).getSource().get(ID).toString();
                System.out.println(result);
                jedis.set("gex:data:user_id_by_username:" + value, result);
            }


            /*response = client.prepareSearch(INDEX).setSearchType(SearchType.QUERY_AND_FETCH).setFetchSource(new String[]{ID}, null)
                    .setTypes(TYPE).setQuery(QueryBuilders.termsQuery("name", name)).execute().actionGet();
            if (response.getHits().getTotalHits() == 0) {
                IndexResponse res = client.prepareIndex(INDEX, TYPE)
                        .setSource(jsonBuilder().startObject()
                                .field("name", name).field("title", name).field("enabled", 1).
                                        field("visible_client", 0).field("need_notify", 0).endObject()
                        )
                        .execute()
                        .actionGet();
            } else {
                System.out.println(response.getHits().getAt(0).getSource().get(ID).toString());
            }*/
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
