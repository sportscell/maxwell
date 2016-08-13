package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MaxwellMasterRecovery {
	private final MaxwellContext context;
	private final Long masterServerID, targetHeartbeat;
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMasterRecovery.class);

	public MaxwellMasterRecovery(MaxwellContext context, Long masterServerID, Long targetHeartbeat) {
		this.context = context;
		this.masterServerID = masterServerID;
		this.targetHeartbeat = targetHeartbeat;
	}

	public BinlogPosition recover() throws SQLException {
		String recoveryMsg = String.format("old-server-id %d, heartbeat %d", masterServerID, targetHeartbeat);
		LOGGER.info("attempting to recover from master-change: " + recoveryMsg);

		List<BinlogPosition> list = getBinlogInfo();
		for ( int i = list.size() - 1; i >= 0 ; i-- ) {
			BinlogPosition position = list.get(i);
			LOGGER.debug("scanning binlog: " + position);

			//MaxwellReplicator replicator = new MaxwellReplicator()
		}

		LOGGER.warn("Could not recover from master-change: " + recoveryMsg);
		return null;
	}

	/**
	 * fetch a list of binlog postiions representing the start of each binlog file
	 *
	 * @return a list of binlog positions to attempt recovery at
	 * */

	private List<BinlogPosition> getBinlogInfo() throws SQLException {
		ArrayList<BinlogPosition> list = new ArrayList<>();
		try ( Connection c = context.getReplicationConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SHOW BINARY LOGS");
			while ( rs.next() ) {
				list.add(BinlogPosition.at(4, rs.getString("Log_name")));
			}
		}
		return list;
	}
}
