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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.XAException;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.index.base.AbstractCommand;
import org.neo4j.index.base.AbstractIndex;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.TxData;
import org.neo4j.index.base.keyvalue.KeyValueCommand;
import org.neo4j.index.base.keyvalue.KeyValueCommand.AddCommand;
import org.neo4j.index.base.keyvalue.KeyValueTransaction;
import org.neo4j.index.base.keyvalue.KeyValueTxData;
import org.neo4j.index.base.keyvalue.OneToOneTxData;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

class RedisTransaction extends KeyValueTransaction
{
    private Jedis redisResource;
    private Transaction transaction;
    private Jedis readOnlyRedisResource;
    
    RedisTransaction( int identifier, XaLogicalLog xaLog,
        RedisDataSource dataSource )
    {
        super( identifier, xaLog, dataSource );
    }
    
    @Override
    protected RedisDataSource getDataSource()
    {
        return (RedisDataSource) super.getDataSource();
    }
    
    @Override
    protected void doPrepare() throws XAException
    {
        super.doPrepare();
        RedisDataSource dataSource = getDataSource();
        acquireRedisTransaction();
        for ( Map.Entry<IndexIdentifier, Collection<AbstractCommand>> entry : getCommands().entrySet() )
        {
            IndexIdentifier identifier = entry.getKey();
            IndexType indexType = dataSource.getIndexType( identifier );
            
            for ( AbstractCommand command : entry.getValue() )
            {
                String indexName = identifier.getIndexName();
                if ( command instanceof KeyValueCommand.CreateIndexCommand )
                {
                    dataSource.getIndexStore().setIfNecessary( identifier.getEntityType(),
                            indexName, ((KeyValueCommand.CreateIndexCommand) command).getConfig() );
                    continue;
                }
                
                KeyValueCommand kvCommand = (KeyValueCommand) command;
                String commandKey = kvCommand.getKey();
                String commandValue = kvCommand.getValue();
                long id = kvCommand.getEntityId();
                
                // TODO Make the command apply itself instead of this if-else-thingie
                if ( kvCommand instanceof KeyValueCommand.AddCommand )
                {
                    indexType.add( transaction, dataSource, identifier, (AddCommand) kvCommand );
                }
                else if ( kvCommand instanceof KeyValueCommand.RemoveCommand )
                {
                    if ( commandKey == null && commandValue == null )
                    {
                        indexType.removeEntity( transaction, this, dataSource, identifier, id );
                    }
                    else if ( commandValue == null )
                    {
                        indexType.removeEntityKey( transaction, this, dataSource, identifier, commandKey, id );
                    }
                    else
                    {
                        indexType.removeEntityKeyValue( transaction, dataSource, identifier, commandKey, commandValue, id );
                    }
                }
                else if ( kvCommand instanceof KeyValueCommand.DeleteIndexCommand )
                {
                    // TODO this doesn't really scale... getting all the keys for an
                    // index can potentially eat up the entire heap. Consider replacing with a list.

                    for (String indexKey : getMembersFromOutsideTransaction( dataSource.formRedisKeyForIndex( identifier ) ))
                    {
                        transaction.del(indexKey);
                    }
                    transaction.del(indexName);
                }
            }
        }
        closeTxData();
    }

    Set<String> getMembersFromOutsideTransaction( String indexName )
    {
        readOnlyRedisResource = readOnlyRedisResource != null ?
                readOnlyRedisResource : getDataSource().acquireResource();
        return readOnlyRedisResource.smembers( indexName );
    }

    private void acquireRedisTransaction( )
    {
        redisResource = getDataSource().acquireResource();
        transaction = redisResource.multi();
    }
    
    @Override
    protected void doCommit()
    {
        // Needed during recovery only
        if ( isRecovered() )
        {
            acquireRedisTransaction();
        }
        
        try
        {
            transaction.exec();
        }
        finally
        {
            releaseResourceIfNecessary( redisResource );
            releaseResourceIfNecessary( readOnlyRedisResource );
        }
    }

    @Override
    protected void doRollback()
    {
        try
        {
            super.doRollback();
            if ( transaction != null )
            {
                transaction.discard();
            }
        }
        finally
        {
            releaseResourceIfNecessary( redisResource );
            releaseResourceIfNecessary( readOnlyRedisResource );
        }
    }
    
    @Override
    public <T extends PropertyContainer> Collection<Long> getRemovedIds( AbstractIndex<T> index,
            String key, Object value )
    {
        TxData removed = removedTxDataOrNull( index );
        if ( removed == null )
        {
            return Collections.emptySet();
        }
        Set<Long> ids = removed.get( key, value );
        Collection<Long> orphans = ((KeyValueTxData)removed).getOrphans( key );
        return merge( ids, orphans );
    }
    
    @Override
    protected TxData newTxData( IndexIdentifier identifier, TxDataType txDataType )
    {
        return txDataType == TxDataType.ADD && getDataSource().getIndexType( identifier ) == IndexType.single_value ?
            new OneToOneTxData() : super.newTxData( identifier, txDataType );
    }

    private void releaseResourceIfNecessary( Jedis resource )
    {
        if ( resource != null )
        {
            getDataSource().releaseResource( resource );
        }
    }
}
