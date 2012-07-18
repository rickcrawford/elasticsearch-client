package com.typeahead.dropwizard.elasticsearch;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Elasticsearch configuration file
 * @author rickcrawford
 *
 */
public class ElasticSearchConfiguration {

	@NotEmpty
	@JsonProperty
	private String host;
	
	@Min(9300)
	@Max(9399)
	@JsonProperty
	private int port = 9300;
	
	
	public ElasticSearchConfiguration() {}
	
	public ElasticSearchConfiguration(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
