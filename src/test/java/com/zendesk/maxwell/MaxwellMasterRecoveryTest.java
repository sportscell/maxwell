package com.zendesk.maxwell;

import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

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

		MaxwellTestSupport.setupSchema(masterServer);
	}

	private MaxwellContext getContext(int port) {
		MaxwellConfig config = new MaxwellConfig();
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.producerType = "stdout";
		config.bootstrapperType = "none";
		config.databaseName = "maxwell";


		config.validate();
		return new MaxwellContext(config);
	}

	@Test
	public void testBasicRecovery() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into shard_1.minimal set account_id = 1, text_field='row 1'"};
		list = MaxwellTestSupport.getRowsForSQL(masterServer, getContext(masterServer.getPort()), null, input, null, false);
		assertThat(list.size(), is(1));

		masterServer.execute("insert into shard_1.minimal set account_id = 1, text_field='row 2'");
		String empty[] = {};
		
		list = MaxwellTestSupport.getRowsForSQL(slaveServer, getContext(slaveServer.getPort()), null, empty, null, false);
		assertThat(list.size(), is(1));
	}

}
