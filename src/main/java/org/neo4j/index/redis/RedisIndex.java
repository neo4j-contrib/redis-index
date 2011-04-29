/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
