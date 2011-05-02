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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.base.AbstractIndexImplementation;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.OneToOneIndex;

import redis.clients.jedis.Jedis;

public abstract class RedisSingleValueIndex<T extends PropertyContainer> extends OneToOneIndex<T>
{
    protected RedisSingleValueIndex( AbstractIndexImplementation provider, IndexIdentifier identifier )
    {
        super( provider, identifier );
    }

    @Override
    protected RedisIndexImplementation getProvider()
    {
        return (RedisIndexImplementation) super.getProvider();
    }
    
    @Override
    protected Long getFromDb( String key, Object value )
    {
        RedisDataSource dataSource = getProvider().dataSource();
        Jedis resource = dataSource.acquireResource();
        try
        {
            // TODO Return lazy iterator instead of converting all values
            // here and now?
            String redisKey = dataSource.formRedisKeyForKeyValue( getIdentifier(), key, value.toString() );
            String stringId = resource.get( redisKey );
            if ( stringId != null && !stringId.equals( "nil" ) )
            {
                return Long.valueOf( stringId );
            }
            return null;
        }
        finally
        {
            dataSource.releaseResource( resource );
        }
    }

    static class NodeIndex extends RedisSingleValueIndex<Node>
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

    static class RelationshipIndex extends RedisSingleValueIndex<Relationship> implements org.neo4j.graphdb.index.RelationshipIndex
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
