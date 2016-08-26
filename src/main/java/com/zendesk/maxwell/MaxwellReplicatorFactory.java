package com.zendesk.maxwell;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.SchemaStore;


/**
 * Created by min on 2016. 8. 26..
 */
public class MaxwellReplicatorFactory {

    public static MaxwellReplicator getMaxwellReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws Exception {

        try {
            if (ctx.getConfig().runningMode == MaxwellRunningMode.SINGLE_MODE) {
                return new MaxwellReplicator(schemaStore, producer, bootstrapper, ctx, ctx.getInitialPosition());
            } else if (ctx.getConfig().runningMode == MaxwellRunningMode.ACTIVE_STANDBY_MODE) {
                return new HAMaxwellReplicator(schemaStore, producer, bootstrapper, ctx, ctx.getInitialPosition());
            } else {
                throw new IllegalArgumentException("No matched Running Mode");
            }

        } catch (Exception e) {
            throw new RuntimeException("MaxwellReplicator initialize failed", e);
        }

    }
}
