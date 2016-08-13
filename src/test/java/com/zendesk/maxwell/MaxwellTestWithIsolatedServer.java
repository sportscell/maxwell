package com.zendesk.maxwell;

import java.sql.SQLException;
import java.util.*;
import org.junit.*;


public class MaxwellTestWithIsolatedServer {
	protected static MysqlIsolatedServer server;

	@BeforeClass
	public static void setupTest() throws Exception {
		server = MaxwellTestSupport.setupServer();
	}

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, filter, input);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input, String[] before) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, null, filter, input, before, false);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, null, null, input, null, false);
	}

	protected List<RowMap> getRowsForSQLTransactional(String[] input) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, null, null, input, null, true);
	}

	protected void runJSON(String filename) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename);
	}

	protected MaxwellContext buildContext() throws SQLException {
		return MaxwellTestSupport.buildContext(server.getPort(), null, null);
	}

	protected MaxwellContext buildContext(BinlogPosition p) throws SQLException {
		return MaxwellTestSupport.buildContext(server.getPort(), p, null);
	}
}
