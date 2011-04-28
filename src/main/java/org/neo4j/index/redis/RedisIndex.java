package org.neo4j.index.redis;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.KeyValueIndex;

import redis.clients.jedis.Jedis;

public abstract class RedisIndex<T extends PropertyContainer> extends KeyValueIndex<T>
{
    RedisIndex( RedisIndexImplementation provider, IndexIdentifier identifier )
    {
        super( provider, identifier );
    }
    
    @Override
    protected RedisIndexImplementation getProvider()
    {
        return (RedisIndexImplementation) super.getProvider();
    }
    
    @Override
    protected void getFromDb( List<Long> ids, String key, Object value )
    {
        // TODO get from redis and add to "ids" list
        RedisDataSource dataSource = getProvider().dataSource();
        Jedis resource = dataSource.acquireResource();
        try
        {
            String redisKey = dataSource.formRedisKey( getIdentifier().getIndexName(),
                    key, value.toString() );
            // TODO Return lazy iterator instead of converting all values here and now?
            Set<String> idsFromRedis = resource.smembers( redisKey );
            for ( String stringId : idsFromRedis )
            {
                ids.add( Long.valueOf( stringId ) );
            }
        }
        finally
        {
            dataSource.releaseResource( resource );
        }
    }
    
    protected abstract T idToEntity( Long id );
    
    static class NodeIndex extends RedisIndex<Node>
    {
        NodeIndex( RedisIndexImplementation provider, IndexIdentifier identifier )
        {
            super( provider, identifier );
        }

        @Override
        protected Node idToEntity( Long id )
        {
            return getProvider().graphDb().getNodeById( id );
        }
    }

    static class RelationshipIndex extends RedisIndex<Relationship> implements org.neo4j.graphdb.index.RelationshipIndex
    {
        RelationshipIndex( RedisIndexImplementation provider, IndexIdentifier identifier )
        {
            super( provider, identifier );
        }

        @Override
        protected Relationship idToEntity( Long id )
        {
            return getProvider().graphDb().getRelationshipById( id );
        }

        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
                Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }

        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }

        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }
    }
}
