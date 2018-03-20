package com.beercafeguy.rest.RestTemplateClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@SpringBootApplication
public class RestTemplateClientApplication implements CommandLineRunner{


    private static final Logger log = LoggerFactory.getLogger(RestTemplateClientApplication.class);

    @Autowired
    RestTemplate restTemplate;

    public static void main(String[] args) {
        SpringApplication.run(RestTemplateClientApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting REST client.");
        log.info("Get topic list from kafka rest proxy...");
        System.out.println(restTemplate.getForObject("http://localhost:8082/topics", List.class));
        HttpHeaders httpHeaders = restTemplate
                .headForHeaders("http://localhost:8082/topics");
        log.info("Header ContentType:"+httpHeaders.getContentType());
        log.info("Calling topic details method");
        //printTopicDetails();
       // postAvroData();
        postBinaryData();
        System.exit(0);
    }

    private void printTopicDetails() throws Exception{
        ArrayList topics=restTemplate.getForObject("http://localhost:8082/topics", ArrayList.class);
        log.info("Topics:"+topics);
        for (Object topic: topics) {
            log.info("Topic name:"+ (String)topic);
            String topicURL="http://localhost:8082/topics/"+(String)topic;
            //String topicDetails=restTemplate.getForObject(topicURL,String.class);
            ResponseEntity<String> response
                    = restTemplate.getForEntity(topicURL, String.class);
            log.info("HTTP Status:"+response.getStatusCode());
            log.info("Topic details for topic "+topic+" : "+response.getBody());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(response.getBody());
        }
    }

    private void postAvroData(){
        log.info("Inside postData method");
        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.avro.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json");
        //List<MediaType> accepts=new ArrayList<MediaType>();
        //accepts.add(new MediaType("application/vnd.kafka.v2+json"));
        //accepts.add(new MediaType("application/vnd.kafka+json"));
        //accepts.add(new MediaType("application/json"));
        //headers.setAccept(accepts);

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"value_schema\": \"{\\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"User\\\", \\\"fields\\\": [{\\\"name\\\": \\\"name\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\" :\\\"age\\\",  \\\"type\\\": [\\\"null\\\",\\\"int\\\"]}]}\",\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"value\": {\"name\": \"Hem Chandra\", \"age\": {\"int\": 25} }\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": {\"name\": \"Aman Chauhan\", \"age\": {\"int\": 28} },\n" +
                "      \"partition\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-avro", entity , String.class );
        log.info("Response:"+response.getStatusCode());

    }


    private void postBinaryData(){

        log.info("Inside postBinaryData method");
        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.binary.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
        //List<MediaType> accepts=new ArrayList<MediaType>();
        //accepts.add(new MediaType("application/vnd.kafka.v2+json"));
        //accepts.add(new MediaType("application/vnd.kafka+json"));
        //accepts.add(new MediaType("application/json"));
        //headers.setAccept(accepts);

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"key\": \"MTA=\",\n" +
                "      \"value\": \"S2FyYW4gQmFuc2Fs\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"TXVkaXQgS2F1bA==\",\n" +
                "      \"partition\": 0\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"UG9vamEgQmlzaHQ=\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-binary", entity , String.class );
        log.info("Response Code:"+response.getStatusCode());
        log.info("Response Body:"+response.getBody());

    }


    private void postJsonData(){

        log.info("Inside postJsonData method");
        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.binary.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
        //List<MediaType> accepts=new ArrayList<MediaType>();
        //accepts.add(new MediaType("application/vnd.kafka.v2+json"));
        //accepts.add(new MediaType("application/vnd.kafka+json"));
        //accepts.add(new MediaType("application/json"));
        //headers.setAccept(accepts);

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"key\": \"MQ==\",\n" +
                "      \"value\": \"SGVtIENoYW5kcmE=\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"QW1hbiBDaGF1aGFu\",\n" +
                "      \"partition\": 0\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"QW5rdXIgRGFz\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-binary", entity , String.class );
        log.info("Response:"+response.getStatusCode());

    }
}
