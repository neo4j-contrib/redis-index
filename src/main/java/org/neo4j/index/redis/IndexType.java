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

import java.util.Set;

import org.neo4j.graphdb.Relationship;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.KeyValueCommand.AddCommand;

import redis.clients.jedis.Transaction;

public enum IndexType
{
    multiple_values
    {
        public void add( Transaction transaction, RedisDataSource dataSource,
                IndexIdentifier identifier, AddCommand command )
        {
            String commandKey = command.getKey();
            String commandValue = command.getValue();
            String keyValueKey = dataSource.formRedisKeyForKeyValue( identifier, commandKey, commandValue );
            long id = command.getEntityId();
            String entityAndKeyRemovalKey = dataSource.formRedisKeyForEntityAndKeyRemoval(
                    identifier, commandKey, id );
            String entityRemovalKey = dataSource.formRedisKeyForEntityRemoval( identifier, id );
            
            transaction.sadd( keyValueKey, "" + id );
            transaction.sadd( entityAndKeyRemovalKey, commandValue );
            transaction.sadd( entityRemovalKey, commandKey );
            
            // For future deletion of the index
            String indexKey = dataSource.formRedisKeyForIndex( identifier );
            transaction.sadd( indexKey, keyValueKey );
            transaction.sadd( indexKey, entityAndKeyRemovalKey );
            transaction.sadd( indexKey, entityRemovalKey );

            addRelationshipData( transaction, dataSource, identifier, command );
        }

        @Override
        public void removeEntity( Transaction transaction, RedisTransaction neo4jTransaction,
                RedisDataSource dataSource, IndexIdentifier identifier, long id )
        {
            String entityRemovalKey = dataSource.formRedisKeyForEntityRemoval( identifier, id );
            Set<String> keys = neo4jTransaction.getMembersFromOutsideTransaction( entityRemovalKey );
            for ( String key : keys )
            {
                removeEntityKey( transaction, neo4jTransaction, dataSource, identifier, key, id );
            }
            transaction.del( entityRemovalKey );
            transaction.srem( identifier.getIndexName(), entityRemovalKey );
        }

        @Override
        public void removeEntityKey( Transaction transaction, RedisTransaction neo4jTransaction,
                RedisDataSource dataSource, IndexIdentifier identifier, String key, long id )
        {
            String entityAndKeyRemovalKey = dataSource.formRedisKeyForEntityAndKeyRemoval( identifier, key, id );
            for ( String value : neo4jTransaction.getMembersFromOutsideTransaction( entityAndKeyRemovalKey ) )
            {
                String keyToRemove = dataSource.formRedisKeyForKeyValue( identifier, key, value );
                transaction.srem( keyToRemove, "" + id );
            }
            transaction.del( entityAndKeyRemovalKey );
            transaction.srem( identifier.getIndexName(), entityAndKeyRemovalKey );
        }

        @Override
        public void removeEntityKeyValue( Transaction transaction, RedisDataSource dataSource,
                IndexIdentifier identifier, String key, String value, long id )
        {
            String keyValueKey = dataSource.formRedisKeyForKeyValue( identifier, key, value );
            String entityAndKeyRemovalKey = dataSource.formRedisKeyForEntityAndKeyRemoval( identifier, key, id );
            transaction.srem( keyValueKey, "" + id );
            transaction.srem( entityAndKeyRemovalKey, value );
            
            // TODO We cannot remove the key from the key set since we don't know
            // if there are more values. Fix later somehow.
            // transaction.srem( entityRemovalKey, commandKey );
            
            // For future deletion of the index
            transaction.srem( identifier.getIndexName(), keyValueKey );
        }
    },
    single_value
    {
        @Override
        public void add( Transaction transaction, RedisDataSource dataSource,
                IndexIdentifier identifier, AddCommand command )
        {
            String keyValueKey = dataSource.formRedisKeyForKeyValue( identifier, command.getKey(), command.getValue() );
            transaction.set( keyValueKey, "" + command.getEntityId() );
            transaction.sadd( dataSource.formRedisKeyForIndex( identifier ), keyValueKey );
            addRelationshipData( transaction, dataSource, identifier, command );
        }

        @Override
        public void removeEntity( Transaction transaction, RedisTransaction neo4jTransaction,
                RedisDataSource dataSource, IndexIdentifier identifier, long id )
        {
            throw new UnsupportedOperationException( "Not supported for one-to-one index type" );
        }

        @Override
        public void removeEntityKey( Transaction transaction, RedisTransaction neo4jTransaction,
                RedisDataSource dataSource, IndexIdentifier identifier, String key, long id )
        {
            throw new UnsupportedOperationException( "Not supported for one-to-one index type" );
        }

        @Override
        public void removeEntityKeyValue( Transaction transaction, RedisDataSource dataSource, IndexIdentifier identifier,
                String key, String value, long id )
        {
            String keyValueKey = dataSource.formRedisKeyForKeyValue( identifier, key, value );
            transaction.del( keyValueKey, "" + id );
            
            // For future deletion of the index
            transaction.srem( identifier.getIndexName(), keyValueKey );
        }
    };
    
    public abstract void add( Transaction transaction, RedisDataSource dataSource,
            IndexIdentifier identifier, AddCommand command );
    
    public abstract void removeEntity( Transaction transaction, RedisTransaction neo4jTransaction,
            RedisDataSource dataSource, IndexIdentifier identifier, long id );

    public abstract void removeEntityKey( Transaction transaction, RedisTransaction neo4jTransaction,
            RedisDataSource dataSource, IndexIdentifier identifier, String key, long id );

    public abstract void removeEntityKeyValue( Transaction transaction, RedisDataSource dataSource,
            IndexIdentifier identifier, String key, String value, long id );

    private static void addRelationshipData( Transaction transaction, RedisDataSource dataSource,
            IndexIdentifier identifier, AddCommand command )
    {
        // For relationship queries
        if (command.getIndexIdentifier().getEntityType() == Relationship.class)
        {
            long id = command.getEntityId();
            transaction.sadd( dataSource.formRedisStartNodeKey(identifier, command.getStartNode()), "" + id );
            transaction.sadd( dataSource.formRedisEndNodeKey(identifier, command.getEndNode()), "" + id );
        }
    }
}
