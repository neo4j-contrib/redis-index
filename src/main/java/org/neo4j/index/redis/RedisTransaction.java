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

import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.XAException;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.index.base.AbstractIndex;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.TxData;
import org.neo4j.index.base.keyvalue.KeyValueTransaction;
import org.neo4j.index.base.keyvalue.KeyValueTxData;
import org.neo4j.index.base.keyvalue.OneToOneTxData;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

class RedisTransaction extends KeyValueTransaction
{
    private Jedis redisResource;
    private Pipeline pipeline;
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
        IndexDefineCommand definitions = getDefinitions( false );
        for ( Map.Entry<IndexIdentifier, Collection<IndexCommand>> entry : getCommands().entrySet() )
        {
            IndexIdentifier identifier = entry.getKey();
            IndexType indexType = dataSource.getIndexType(identifier);
            
            for ( IndexCommand command : entry.getValue() )
            {
                String indexName = identifier.getIndexName();
                if ( command instanceof IndexCommand.CreateCommand )
                {
                    dataSource.getIndexStore().setIfNecessary( identifier.getEntityType(),
                            indexName, ((IndexCommand.CreateCommand) command).getConfig() );
                    continue;
                }
                else if ( command instanceof IndexCommand.DeleteCommand )
                {
                    Set<String> keys = getKeysFromOutsideTransaction( identifier );
                    if ( !keys.isEmpty() )
                    {
                        pipeline.del( keys.toArray( new String[keys.size()] ) );
                    }
                    continue;
                }
                
                IndexCommand indexCommand = (IndexCommand) command;
                byte keyId = indexCommand.getKeyId();
                String commandKey = keyId > 0 ? definitions.getKey( keyId ) : null;
                Object commandValue = indexCommand.getValue();
                long id = indexCommand.getEntityId();
                
                // TODO Make the command apply itself instead of this if-else-thingie
                if ( indexCommand instanceof AddCommand )
                {
                    indexType.add( pipeline, identifier, commandKey, commandValue.toString(), id, 0, 0 );
                }
                else if ( indexCommand instanceof AddRelationshipCommand )
                {
                    AddRelationshipCommand addCommand = (AddRelationshipCommand) indexCommand;
                    indexType.add( pipeline, identifier, commandKey, commandValue.toString(), id,
                            addCommand.getStartNode(), addCommand.getEndNode() );
                }
                else if ( indexCommand instanceof RemoveCommand )
                {
                    if ( commandKey == null && commandValue == null )
                    {
                        indexType.removeEntity( pipeline, this, identifier, id );
                    }
                    else if ( commandValue == null )
                    {
                        indexType.removeEntityKey( pipeline, this, identifier, commandKey, id );
                    }
                    else
                    {
                        indexType.removeEntityKeyValue( pipeline, identifier, commandKey,
                                commandValue.toString(), id );
                    }
                }
                else
                {
                    throw new IllegalArgumentException( "Unknown command " + indexCommand );
                }
            }
        }
        closeTxData();
    }

    Set<String> getKeysFromOutsideTransaction( IndexIdentifier identifier )
    {
        readOnlyRedisResource = readOnlyRedisResource != null ?
                readOnlyRedisResource : getDataSource().acquireResource();
        String pattern = getDataSource().formRedisIndexPattern(identifier);
        return readOnlyRedisResource.keys(pattern);
    }

    Set<String> getMembersFromOutsideTransaction( String indexName )
    {
        readOnlyRedisResource = readOnlyRedisResource != null ?
                readOnlyRedisResource : getDataSource().acquireResource();
        return readOnlyRedisResource.smembers(indexName);
    }

    private void acquireRedisTransaction( )
    {
        redisResource = getDataSource().acquireResource();
        
        // select the target database before starting the pipeline
        int targetDatabase = RedisDataSource.getTargetDatabase();
        if (targetDatabase != RedisDataSource.DEFAULT_DATABASE)
        {
            redisResource.select(targetDatabase);
        }

        pipeline = redisResource.pipelined();
        pipeline.multi();
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
            pipeline.exec();
            pipeline.execute();
        }
        catch ( JedisConnectionException e )
        {
            if ( e.getCause() instanceof SocketTimeoutException )
            {
                // TODO Issue warning to log
                System.out.println( "TODO log properly: Read timeout" );
            }
            else
            {
                throw e;
            }
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
            if ( pipeline != null )
            {
                pipeline.discard();
                pipeline.execute();
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
