package com.zendesk.maxwell;

import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.Pair;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class MaxwellMasterRecoveryTest {
	private static MysqlIsolatedServer masterServer, slaveServer;

	@BeforeClass
	public static void setupSlaveServer() throws Exception {
		masterServer = new MysqlIsolatedServer();
		masterServer.boot();
		SchemaStoreSchema.ensureMaxwellSchema(masterServer.getConnection(), "maxwell");

		slaveServer = MaxwellTestSupport.setupServer("--server_id=12345");
		slaveServer.setupSlave(masterServer.getPort());

		MaxwellTestSupport.setupSchema(masterServer, false);
	}

	private MaxwellConfig getConfig(int port) {
		MaxwellConfig config = new MaxwellConfig();
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		return config;
	}

	private MaxwellContext getContext(int port) throws SQLException {
		MaxwellConfig config = getConfig(port);
		config.validate();
		return new MaxwellContext(config);
	}

	@Test
	public void testBasicRecovery() throws Exception {
		List<RowMap> list;

		MaxwellContext masterContext = getContext(masterServer.getPort());
		MaxwellContext slaveContext  = getContext(slaveServer.getPort());


		// send a heartbeat into the replication stream.
		MysqlPositionStore store = masterContext.getPositionStore();
		store.set(BinlogPosition.capture(masterServer.getConnection()));
		masterContext.getPositionStore().heartbeat();

		String input[] = new String[5000];
		for ( int i = 0 ; i < 5000; i++ )
			input[i] = String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i, i);

		// now the getRowsForSQL will trigger a read of that heartbeat.

		MaxwellTestSupport.getRowsForSQL(masterServer, getContext(masterServer.getPort()), null, input, null, false);

		Thread.sleep(3000); // try to make sure master is flushed -> slave
		BinlogPosition masterPosition = BinlogPosition.capture(masterServer.getConnection());
		BinlogPosition slavePosition = BinlogPosition.capture(slaveServer.getConnection());

		System.out.println("master: " + masterPosition + " slave: " + slavePosition);

		MaxwellMasterRecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		MaxwellConfig config = getConfig(masterServer.getPort());
		MaxwellMasterRecovery recovery = new MaxwellMasterRecovery(
			config.maxwellMysql,
			config.databaseName,
			new MysqlSchemaStore(slaveContext, null),
			slaveContext.getReplicationConnectionPool(),
			recoveryInfo
		);

		BinlogPosition recoveredPosition = recovery.recover();
		assertEquals(slavePosition, recoveredPosition);
	}

}
