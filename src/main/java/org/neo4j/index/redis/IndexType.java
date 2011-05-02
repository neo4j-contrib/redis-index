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

import static org.neo4j.index.redis.RedisDataSource.formRedisEndNodeKey;
import static org.neo4j.index.redis.RedisDataSource.formRedisKeyForEntityAndKeyRemoval;
import static org.neo4j.index.redis.RedisDataSource.formRedisKeyForEntityRemoval;
import static org.neo4j.index.redis.RedisDataSource.formRedisKeyForIndex;
import static org.neo4j.index.redis.RedisDataSource.formRedisKeyForKeyValue;
import static org.neo4j.index.redis.RedisDataSource.formRedisStartNodeKey;

import java.util.Set;

import org.neo4j.graphdb.Relationship;
import org.neo4j.index.base.IndexIdentifier;

import redis.clients.jedis.Pipeline;

public enum IndexType
{
    multiple_values
    {
        @Override
        public void add( Pipeline pipeline, IndexIdentifier identifier, String key, String value,
                long id, long startNode, long endNode )
        {
            String keyValueKey = formRedisKeyForKeyValue( identifier, key, value );
            String entityAndKeyRemovalKey = formRedisKeyForEntityAndKeyRemoval(
                    identifier, key, id );
            String entityRemovalKey = formRedisKeyForEntityRemoval( identifier, id );
            
            pipeline.sadd( keyValueKey, "" + id );
            pipeline.sadd( entityAndKeyRemovalKey, value );
            pipeline.sadd( entityRemovalKey, key );
            
            // For future deletion of the index
            String indexKey = formRedisKeyForIndex( identifier );
            pipeline.sadd( indexKey, keyValueKey );
            pipeline.sadd( indexKey, entityAndKeyRemovalKey );
            pipeline.sadd( indexKey, entityRemovalKey );

            // For relationship queries
            if ( identifier.getEntityType() == Relationship.class )
            {
                pipeline.sadd( formRedisStartNodeKey( identifier, startNode ), "" + id );
                pipeline.sadd( formRedisEndNodeKey( identifier, endNode ), "" + id );
            }
        }

        @Override
        public void removeEntity( Pipeline pipeline, RedisTransaction neo4jTransaction,
                IndexIdentifier identifier, long id )
        {
            String entityRemovalKey = formRedisKeyForEntityRemoval( identifier, id );
            Set<String> keys = neo4jTransaction.getMembersFromOutsideTransaction( entityRemovalKey );
            for ( String key : keys )
            {
                removeEntityKey( pipeline, neo4jTransaction, identifier, key, id );
            }
            pipeline.del( entityRemovalKey );
            pipeline.srem( identifier.getIndexName(), entityRemovalKey );
        }

        @Override
        public void removeEntityKey( Pipeline pipeline, RedisTransaction neo4jTransaction,
                IndexIdentifier identifier, String key, long id )
        {
            String entityAndKeyRemovalKey = formRedisKeyForEntityAndKeyRemoval( identifier, key, id );
            for ( String value : neo4jTransaction.getMembersFromOutsideTransaction( entityAndKeyRemovalKey ) )
            {
                String keyToRemove = formRedisKeyForKeyValue( identifier, key, value );
                pipeline.srem( keyToRemove, "" + id );
            }
            pipeline.del( entityAndKeyRemovalKey );
            pipeline.srem( identifier.getIndexName(), entityAndKeyRemovalKey );
        }

        @Override
        public void removeEntityKeyValue( Pipeline pipeline,
                IndexIdentifier identifier, String key, String value, long id )
        {
            String keyValueKey = formRedisKeyForKeyValue( identifier, key, value );
            String entityAndKeyRemovalKey = formRedisKeyForEntityAndKeyRemoval( identifier, key, id );
            pipeline.srem( keyValueKey, "" + id );
            pipeline.srem( entityAndKeyRemovalKey, value );
            
            // TODO We cannot remove the key from the key set since we don't know
            // if there are more values. Fix later somehow.
            // transaction.srem( entityRemovalKey, commandKey );
            
            // For future deletion of the index
            pipeline.srem( identifier.getIndexName(), keyValueKey );
        }
    },
    single_value
    {
        @Override
        public void add( Pipeline pipeline, IndexIdentifier identifier, String key, String value,
                long id, long startNode, long endNode )
        {
            String keyValueKey = formRedisKeyForKeyValue( identifier, key, value );
            pipeline.set( keyValueKey, "" + id );
            pipeline.sadd( formRedisKeyForIndex( identifier ), keyValueKey );
        }

        @Override
        public void removeEntity( Pipeline pipeline, RedisTransaction neo4jTransaction,
                IndexIdentifier identifier, long id )
        {
            throw new UnsupportedOperationException( "Not supported for one-to-one index type" );
        }

        @Override
        public void removeEntityKey( Pipeline pipeline, RedisTransaction neo4jTransaction,
                IndexIdentifier identifier, String key, long id )
        {
            throw new UnsupportedOperationException( "Not supported for one-to-one index type" );
        }

        @Override
        public void removeEntityKeyValue( Pipeline pipeline, IndexIdentifier identifier,
                String key, String value, long id )
        {
            String keyValueKey = formRedisKeyForKeyValue( identifier, key, value );
            pipeline.del( keyValueKey, "" + id );
            
            // For future deletion of the index
            pipeline.srem( identifier.getIndexName(), keyValueKey );
        }
    };
    
    public abstract void add( Pipeline pipeline, IndexIdentifier identifier, String key, String value,
            long id, long startNode, long endNode );
    
    public abstract void removeEntity( Pipeline pipeline, RedisTransaction neo4jTransaction,
            IndexIdentifier identifier, long id );

    public abstract void removeEntityKey( Pipeline pipeline, RedisTransaction neo4jTransaction,
            IndexIdentifier identifier, String key, long id );

    public abstract void removeEntityKeyValue( Pipeline pipeline,
            IndexIdentifier identifier, String key, String value, long id );
}
