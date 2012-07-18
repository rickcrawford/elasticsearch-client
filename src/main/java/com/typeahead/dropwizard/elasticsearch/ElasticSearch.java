package com.typeahead.dropwizard.elasticsearch;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.dropwizard.logging.Log;

/**
 * ElasticSearch managed object for DropWizard
 * @author rickcrawford
 *
 */
public class ElasticSearch extends TransportClient implements Managed {

	private static final Log LOG = Log.forClass(ElasticSearch.class);
	
	
	/**
	 * initialize a new elastic search instance
	 * @param settings
	 * @param host
	 * @param port
	 */
	public ElasticSearch(Settings settings, String host, int port) {
		super(settings);
		LOG.debug("initializing elastic search {}", settings, host, port);
		addTransportAddress(new InetSocketTransportAddress(host, port));
	}
	
	
	
	/**
	 * helper function to get the current cluster status. RED, YELLOW or GREEN
	 * @return ClusterHealthStatus
	 * @throws Exception
	 */
	public ClusterHealthStatus ping() throws Exception {
		LOG.debug("ping request for health...");
		
		ClusterHealthResponse response = admin()
				.cluster()
				.prepareHealth(new String[]{})
				.setTimeout(new TimeValue(50000))
				.execute()
				.actionGet();
		
		if (response.isTimedOut()) {
			LOG.error("health check timeout!");
			throw new Exception("timeout");
		}
		
		return response.status();
	}
	
	
	public void start() throws Exception {
		LOG.debug("starting!");
	}
	
	
	public void stop() throws Exception {
		LOG.debug("stopping!");
		this.close();
	}


}
