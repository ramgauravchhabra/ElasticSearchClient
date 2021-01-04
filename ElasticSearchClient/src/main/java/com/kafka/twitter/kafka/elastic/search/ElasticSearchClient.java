package com.kafka.twitter.kafka.elastic.search;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class ElasticSearchClient {

	public static RestHighLevelClient createElasticSearchClient(){

		//////////////////////////
		/////////// IF YOU USE LOCAL ELASTICSEARCH
		//////////////////////////

		//  String hostname = "localhost";
		//  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


		//////////////////////////
		/////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
		//////////////////////////

		// replace with your own credentials
		// https://noq8kcr4v8:s8suhp85bd@gc-kafka-poc-8635905494.ap-southeast-2.bonsaisearch.net:443
		String hostname = "gc-kafka-poc-8635905494.ap-southeast-2.bonsaisearch.net:443"; // localhost or bonsai url
		String username = "noq8kcr4v8"; // needed only for bonsai
		String password = "s8suhp85bd"; // needed only for bonsai

		// credentials provider help supply username and password
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	// For testing purpose
	public static void main(String s[]) throws IOException {
		// Create Elastic Search Client
		RestHighLevelClient restHighLevelClient = createElasticSearchClient();
		
		// JSON String
		String jsonString = "{ \"foo\":\"bar\" }";
		
		// Create Index Request where actual data got stored
		IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
		
		// Sending request and getting response now
		IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
		String id = indexResponse.getId();
		System.out.println("*** Elastic Search : Saved Id : "+id);
		
		// CLient close gracefully
		restHighLevelClient.close();
	}
}
