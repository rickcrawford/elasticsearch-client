package com.typeahead.dropwizard.elasticsearch;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import com.yammer.dropwizard.config.Environment;

/**
 * Factory for creating a managed ElasticSearch object
 * @author rickcrawford
 *
 */
public class ElasticSearchFactory {

	private Environment environment;
	
	/**
	 * Create a new ElasticSearchFactory
	 * @param environment - current dropwizard environment object
	 */
	public ElasticSearchFactory(Environment environment) {
		this.environment = environment;
	}
	
	
	/**
	 * initializes an ElasticSearch client
	 * @param configuration
	 * @param name
	 * @return
	 */
	public ElasticSearch build(ElasticSearchConfiguration configuration, String name)  {
		final String host = configuration.getHost();
		final int port = configuration.getPort();

		final Settings settings = ImmutableSettings.settingsBuilder().build();
		final ElasticSearch conn = new ElasticSearch(settings, host, port);
				
		environment.manage(conn);
		environment.addHealthCheck(new ElasticSearchHealthCheck(conn, name));
		
		return conn;
	}
	
}
