package org.apache.pulsar.replicator.api;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class ReplicatorConfig {

	private String topicName;
	private String brokerServiceUrl;
	private String zkServiceUrl;
	private ReplicatorType replicatorType;
	
}
