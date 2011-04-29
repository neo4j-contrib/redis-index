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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.base.IndexBaseXaConnection;
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
    
    public void remove( T entity, String key )
    {
        IndexBaseXaConnection connection = getConnection();
        connection.remove( this, entity, key, null );
    }
    
    public void remove( T entity )
    {
        IndexBaseXaConnection connection = getConnection();
        connection.remove( this, entity, null, null );
    }
    
    @Override
    public IndexHits<T> get(String key, Object value) {
        return read(new EntityGetCallback(key, value));

    }

    protected abstract T idToEntity( Long id );
    
    abstract class AbstractReadCallback extends ReadCallback
    {
        protected AbstractReadCallback( String key, Object value )
        {
            super( key, value );
        }
        
        @Override
        protected void update( List<Long> ids, Collection<Long> except )
        {
            RedisDataSource dataSource = getProvider().dataSource();
            Jedis resource = dataSource.acquireResource();
            try
            {
                // TODO Return lazy iterator instead of converting all values
                // here and now?
                Set<String> idsFromRedis = getIdsFromRedis( dataSource, resource );
                for ( String stringId : idsFromRedis )
                {
                    Long id = Long.valueOf( stringId );
                    if ( !except.contains( id ) )
                    {
                        ids.add( id );
                    }
                }
            }
            finally
            {
                dataSource.releaseResource( resource );
            }
        }

        protected abstract Set<String> getIdsFromRedis( RedisDataSource dataSource, Jedis resource );
    }

    class EntityGetCallback extends AbstractReadCallback
    {
        protected EntityGetCallback( String key, Object value )
        {
            super( key, value );
        }
        
        @Override
        protected Set<String> getIdsFromRedis( RedisDataSource dataSource, Jedis resource )
        {
            String redisKey = dataSource.formRedisKeyForKeyValue( getIdentifier(), key, value.toString() );
            return resource.smembers( redisKey );
        }
    }

    class RelationshipGetCallback extends AbstractReadCallback
    {
        private final long startNode;
        private final long endNode;

        // TODO handle null values?
        protected RelationshipGetCallback( String key, Object value, Node startNode, Node endNode )
        {
            super( key, value );
            this.startNode = startNode != null ? startNode.getId() : -1;
            this.endNode = endNode != null ? endNode.getId() : -1;
        }
        
        @Override
        protected Set<String> getIdsFromRedis( RedisDataSource dataSource, Jedis resource )
        {
            List<String> keys = new ArrayList<String>( 3 );
            keys.add( dataSource.formRedisKeyForKeyValue( getIdentifier(), key,
                    value.toString() ) );
            if ( startNode != -1 )
            {
                keys.add( dataSource.formRedisStartNodeKey( getIdentifier(),
                        startNode ) );
            }
            if ( endNode != -1 )
            {
                keys.add( dataSource.formRedisEndNodeKey( getIdentifier(),
                        endNode ) );
            }

            // TODO Return lazy iterator instead of converting all values
            // here and now?
            return resource.sinter( keys.toArray( new String[keys.size()] ) );
        }
    }
    
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
            return read(new RelationshipGetCallback(key, valueOrNull, startNodeOrNull, endNodeOrNull));
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
