package com.zendesk.maxwell;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.SchemaStore;

/**
 * Created by min on 2016. 8. 26..
 */
public class HAMaxwellReplicator extends MaxwellReplicator {

    public HAMaxwellReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws Exception {
        super(schemaStore, producer, bootstrapper, ctx, start);
    }

}
