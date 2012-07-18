package com.typeahead.dropwizard.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.junit.Before;
import org.junit.Test;

//import static org.elasticsearch.index.query.IndicesQueryBuilder.*;

public class ElasticSearchTest {

	Client client = null;

	@Before
	public void setup() throws Exception {
		client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));
	}

	@Test
	public void test1() throws Exception {

		IndexResponse response = client
				.prepareIndex("twitter", "tweet", "1")
				.setSource(
						jsonBuilder().startObject().field("user", "rick")
								.field("postDate", new Date())
								.field("message", "another test").endObject())
				.execute().actionGet();

		System.out.println("-test1---------------------");
		System.out.println("id:" + response.getId());
		System.out.println("version:" + response.getVersion());
		System.out.println("----------------------");

		Thread.sleep(1000);

	}

	@Test
	public void test2() throws Exception {
		NodesInfoResponse response = client.admin().cluster()
				.nodesInfo(new NodesInfoRequest().all()).actionGet();

		System.out.println("-test2---------------------");
		System.out.println("cluster name: " + response.getClusterName());
		for (NodeInfo n : response.getNodes()) {
			System.out.println(" - hostname: " + n.getHostname());
			Map<String, String> settings = n.getSettings().getAsMap();
			for (String key : settings.keySet()) {
				System.out.println(" - " + key + ": " + settings.get(key));
			}
		}
		System.out.println("----------------------");

		Thread.sleep(1000);
	}

	
	@Test
	public void testClusterHealth() throws Exception {
		
		ClusterHealthResponse response = client.admin().cluster().prepareHealth(new String[]{}).setTimeout(new TimeValue(50000)).execute().actionGet();
		
		System.out.println("-testClusterHealth---------------------");
		System.out.println("cluster name: " + response.getClusterName());
		System.out.println("active shards: " + response.getActiveShards());
		System.out.println("active primary shards: " + response.getActivePrimaryShards());
		System.out.println("data nodes: " + response.getNumberOfDataNodes());
		System.out.println("nodes: " + response.getNumberOfNodes());
		System.out.println("reloc shards: " + response.getRelocatingShards());
		System.out.println("unassigned shards: " + response.getUnassignedShards());
		System.out.println("initializing shards: " + response.getInitializingShards());
		
		ClusterHealthStatus status = response.status();
		System.out.println("status: " + status);
		System.out.println("----------------------");

		Thread.sleep(1000);
	}

	@Test
	public void test3() throws Exception {
		BulkResponse response = client
				.prepareBulk()
				.add(new IndexRequest("twitter", "tweet", "2")
						.source(jsonBuilder().startObject()
								.field("user", "test1")
								.field("postDate", new Date())
								.field("message", "test2 message").endObject()))
				.add(new IndexRequest("twitter", "tweet", "3")
						.source(jsonBuilder().startObject()
								.field("user", "test3")
								.field("postDate", new Date())
								.field("message", "test3 message").endObject()))
				.add(new IndexRequest("twitter", "tweet", "4")
						.source(jsonBuilder().startObject()
								.field("user", "test4")
								.field("postDate", new Date())
								.field("message", "test4 message").endObject()))
				.add(new IndexRequest("twitter", "tweet", "5")
						.source(jsonBuilder().startObject()
								.field("user", "test5")
								.field("postDate", new Date())
								.field("message", "test5 message").endObject()))
								
								
				.add(new IndexRequest("twitter", "tweet", "6")
						.source(jsonBuilder().startObject()
								.field("user", "rick")
								.field("postDate", new Date())
								.field("message", "another rick message").endObject()))
				.execute().actionGet();
		
		System.out.println("-test3---------------------");
		for (BulkItemResponse resp : response.items()) {
			System.out.println(resp.id() + ":" + resp.version());
		}
		
		System.out.println("----------------------");

		Thread.sleep(1000);
	}

	@Test
	public void test4() throws Exception {
		CountResponse response = client.prepareCount("twitter").execute()
				.actionGet();

		System.out.println("-test4---------------------");
		System.out.println(response.count());
		System.out.println("----------------------");

		Thread.sleep(1000);

	}

	@Test
	public void test5() throws Exception {
		QueryBuilder query = QueryBuilders.termQuery("user", "rick");
		SearchResponse response = client.prepareSearch("twitter").setQuery(query).execute().actionGet();
		
		System.out.println("-test5---------------------");
		SearchHits hits = response.getHits();
		System.out.println("total: " + hits.getTotalHits());
		for (SearchHit hit : hits) {
			System.out.println(hit.getId() + ":" + hit.getSourceAsString() + " score:" + hit.getScore());
		}
		System.out.println("----------------------");		
		
		
		Thread.sleep(1000);
	}
	
	@Test
	public void test6() throws Exception {
		QueryBuilder query = QueryBuilders.disMaxQuery().tieBreaker(0.7f).boost(1.2f)
							.add(QueryBuilders.matchAllQuery());
		
		
		
		SearchResponse response = client.prepareSearch("twitter")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.addFacet(FacetBuilders.termsFacet("Users").allTerms(true).field("user"))
				.setQuery(query)
				.setFrom(0)
				.setSize(3)
				.setExplain(true)
				.execute()
				.actionGet();

		
		
		
		System.out.println("-test6---------------------");
		SearchHits hits = response.getHits();
		System.out.println("total: " + hits.getTotalHits());
		
		TermsFacet facet = response.getFacets().facet(TermsFacet.class, "Users");
		System.out.println("facet: " + facet.getName() + ":" + facet.getTotalCount());
		for (TermsFacet.Entry entry : facet.entries()) {
			System.out.println(" - " + entry.getTerm() + ":" + entry.getCount());
		}
		
		
		for (SearchHit hit : hits) {
			System.out.println(hit.getId() + ":" + hit.getSourceAsString() + " score:" + hit.getScore());
		}
		System.out.println("----------------------");		
		
		
		Thread.sleep(1000);
		
	}
	
	@Test
	public void test7() throws Exception {
//		QueryBuilder query = QueryBuilders.disMaxQuery().tieBreaker(0.7f).boost(1.2f)
//							.add(QueryBuilders.matchAllQuery());

		QueryBuilder query = QueryBuilders.matchAllQuery();
		
		SearchResponse response = client.prepareSearch("twitter")
				.setSearchType(SearchType.SCAN)
				.setScroll(new TimeValue(60000))
				.setQuery(query)
				.setSize(2)
				.setExplain(true)
				.execute()
				.actionGet();

		
		System.out.println("-test7---------------------");
		
		
		
		//Scroll until no hits are returned
		while (true) {
		    response = client.prepareSearchScroll(response.getScrollId())
		    		.setScroll(new TimeValue(600000))
		    		.execute()
		    		.actionGet();
		    for (SearchHit hit : response.getHits()) {
		    	System.out.println(hit.getId() + ":" + hit.getSourceAsString() + " score:" + hit.getScore() + " explain:" + hit.explanation());
			}		    
		    //Break condition: No hits are returned
		    if (response.hits().hits().length == 0) {
		        break;
		    }
		    System.out.println("!!");
		}
		
		System.out.println("----------------------");		
		
		
		Thread.sleep(1000);
		
	}
	
	@Test
	public void test8() throws Exception {
		DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
				.execute().actionGet();

		System.out.println("-test8---------------------");
		System.out.println(response.getId());
		System.out.println("----------------------");

		Thread.sleep(1000);
	}
	
	
	@Test
	public void test9() throws Exception {
		DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest("twitter")).actionGet();

		System.out.println("-test9---------------------");
		System.out.println(response.acknowledged());
		System.out.println("----------------------");

		Thread.sleep(1000);
	}

}
