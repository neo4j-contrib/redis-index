package org.neo4j.index.redis;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.KernelData;

public class RedisIndexProvider extends IndexProvider
{
    public RedisIndexProvider()
    {
        super( RedisIndexImplementation.SERVICE_NAME );
    }

    @Override
    public IndexImplementation load( KernelData kernel )
    {
        try
        {
            return new RedisIndexImplementation( kernel.graphDatabase(), kernel.getConfig() );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw e;
        }
    }
}
