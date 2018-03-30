package org.apache.pulsar.replicator.api;

public interface ReplicatorManager {

	/**
	 * Starts the replicator manager that initialize internal replicator resources.
	 * 
	 * @param config
	 * @throws Exception
	 */
	void start(ReplicatorConfig config) throws Exception;

	/**
	 * Close replicator manager resources.
	 * 
	 * @throws Exception
	 */
	void close() throws Exception;

}
