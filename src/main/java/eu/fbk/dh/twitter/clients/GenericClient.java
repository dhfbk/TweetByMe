package eu.fbk.dh.twitter.clients;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class GenericClient {
    private URL url;

    public GenericClient(String host, String port, String protocol, String address) throws MalformedURLException {
        url = new URL(protocol, host, Integer.parseInt(port), address);
    }

    public String request(String json) throws IOException {

        HttpPost httpPost = new HttpPost(url.toString());
        StringEntity body = new StringEntity(json, StandardCharsets.UTF_8);
        httpPost.setEntity(body);
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        String returnString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        return returnString;
    }

    public static void main(String[] args) {
        try {
            GenericClient converter = new GenericClient("dh-server.fbk.eu", "9206", "http", "");
            converter.request("Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
