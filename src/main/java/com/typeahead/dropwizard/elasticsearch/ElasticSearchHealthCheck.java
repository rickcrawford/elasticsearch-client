package com.typeahead.dropwizard.elasticsearch;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;

import com.yammer.metrics.core.HealthCheck;

/**
 * performs a health check on elasticsearch client
 * @author rickcrawford
 *
 */
public class ElasticSearchHealthCheck extends HealthCheck {
	
	private final ElasticSearch conn;
	
	/**
	 * create a new healthcheck object for ElasticSearch
	 * @param conn - current elasticsearch connection
	 * @param name - name of this instance
	 */
	public ElasticSearchHealthCheck(ElasticSearch conn, String name) {
		super(name + "-elasticsearch");
		this.conn = conn;
	}

	@Override
	protected Result check() throws Exception {
		ClusterHealthStatus status = conn.ping();
		//if the status is red... 
		if (status != ClusterHealthStatus.RED) {
			return Result.healthy("status: " + status);
		}
		return Result.unhealthy("Cluster is currently RED!");
	}



}
