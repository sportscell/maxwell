package com.zendesk.maxwell;

import com.zendesk.maxwell.schema.MysqlPositionStore;
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

		// now the getRowsForSQL will trigger a read of that heartbeat.
		String input[] = {"insert into shard_1.minimal set account_id = 1, text_field='row 1'"};
		list = MaxwellTestSupport.getRowsForSQL(masterServer, getContext(masterServer.getPort()), null, input, null, false);

		BinlogPosition masterPosition = BinlogPosition.capture(masterServer.getConnection());
		BinlogPosition slavePosition = BinlogPosition.capture(slaveServer.getConnection());

		System.out.println("master: " + masterPosition + " slave: " + slavePosition);

		Pair<Long, Long> recoveryInfo = slaveContext.getRecoveryInfo();

		MaxwellMasterRecovery recovery
			= new MaxwellMasterRecovery(slaveContext.getReplicationConnectionPool(), recoveryInfo.getLeft(), recoveryInfo.getRight());

		BinlogPosition recoveredPosition = recovery.recover();
		assertEquals(slavePosition, recoveredPosition);
	}

}
