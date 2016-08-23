package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class Maxwell implements Runnable {
	private MaxwellConfig config;
	private MaxwellContext context;
	private MaxwellReplicator replicator;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) {
		this.config = config;
	}

	private void checkMysqlStates() throws MaxwellCompatibilityError, SQLException {
		try ( Connection connection = this.context.getReplicationConnection();
			  Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
		} catch ( SQLException e ) {
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			throw(e);
		}
	}

	private void initSchemaStore() throws SQLException, IOException, InvalidSchemaError {
		try ( Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);
		}

		try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
			SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
		}
	}

	private boolean initialized = false;

	public void init() throws Exception {
		if ( this.initialized )
			return;

		this.context = new MaxwellContext(this.config);
		this.context.probeConnections();

		checkMysqlStates();
		initSchemaStore();
	}

	public void start() throws Exception {
		init();

		BinlogPosition initialPosition = this.context.getInitialPosition();

		AbstractProducer producer = this.context.getProducer();
		String producerClassName;
		if ( producer == null )
			producerClassName = "no producer";
		else
			producerClassName = producer.getClass().getSimpleName();

		LOGGER.info("Maxwell is booting (" + producerClassName + "), starting at " + initialPosition);

		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initialPosition);
		this.replicator = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initialPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				terminate();
			}
		});

		this.context.start();
		replicator.startReplicator();
	}

	public void doRun() throws Exception {
		start();
		replicator.runLoop();
	}

	public void run() {
		try {
			doRun();
		} catch ( Exception e ) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	public RowMap getRow(long timeout, TimeUnit unit) throws IOException, InterruptedException {
		if ( !this.config.producerType.equals("buffer") )
			throw new IllegalArgumentException("Please use this interface only with a buffered producer");

		BufferedProducer p = (BufferedProducer) this.context.getProducer();
		return p.poll(timeout, unit);
	}

	public void terminate() {
		try {
			// send a final heartbeat through the system
			context.heartbeat();
			Thread.sleep(100);

			if ( this.replicator != null)
				replicator.stopLoop();
		} catch (TimeoutException e) {
			System.err.println("Timed out trying to shutdown maxwell parser thread.");
		} catch (InterruptedException e) {
		} catch (Exception e) {
		}

		if ( this.context != null )
			context.terminate();

		replicator = null;
		context = null;
	}

	public static void main(String[] args) {
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					StaticShutdownCallbackRegistry.invoke();
				}
			});

			MaxwellConfig config = new MaxwellConfig(args);

			if ( config.log_level != null )
				MaxwellLogging.setLevel(config.log_level);

			new Maxwell(config).doRun();
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
