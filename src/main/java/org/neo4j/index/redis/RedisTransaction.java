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
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.XAException;

import org.neo4j.graphdb.Relationship;
import org.neo4j.index.base.AbstractCommand;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.KeyValueCommand;
import org.neo4j.index.base.keyvalue.KeyValueTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

class RedisTransaction extends KeyValueTransaction
{
    private Jedis redisResource;
    private Transaction transaction;
    
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
                String redisKey = dataSource.formRedisKey( indexName,
                        kvCommand.getKey(), kvCommand.getValue() );
                long id = kvCommand.getEntityId();
                
                // TODO Make the command apply instead of this if-else-thingie
                if ( kvCommand instanceof KeyValueCommand.AddCommand )
                {
                    transaction.sadd( redisKey, "" + id );
                    
                    // For future deletion of the index
                    transaction.sadd( indexName, redisKey );

                    // For relationship queries
                    if (kvCommand.getIndexIdentifier().getEntityType() == Relationship.class)
                    {
                        transaction.sadd( "start:" + kvCommand.getStartNode(), "" + id );
                        transaction.sadd( "end:" + kvCommand.getEndNode(), "" + id );
                    }
                }
                else if ( kvCommand instanceof KeyValueCommand.RemoveCommand )
                {
                    transaction.srem( redisKey, "" + id );
                    
                    // For future deletion of the index
                    transaction.srem( indexName, redisKey );
                }
                else if ( kvCommand instanceof KeyValueCommand.DeleteIndexCommand ) {
                    // TODO this doesn't really scale... getting all the keys for an
                    // index can potentially eat up the entire heap. Consider replacing with a list.

                    // acquire a separate redis connection to read the all the index keys.
                    // using the current redisResource is not possible because of the ongoing transaction
                    Jedis readOnlyRedisResource = null;
                    Set<String> members;
                    try {
                        readOnlyRedisResource = getDataSource().acquireResource();
                        members = readOnlyRedisResource.smembers(indexName);
                    } finally {
                        getDataSource().releaseResource(readOnlyRedisResource);
                    }

                    for (String indexKey : members) {
                        transaction.del(indexKey);
                    }

                    transaction.del(indexName);
                }
            }
        }
        closeTxData();
    }

    private void acquireRedisTransaction( )
    {
        redisResource = getDataSource().acquireResource();
        transaction = redisResource.multi();
    }
    
    @Override
    protected void doCommit()
    {
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
            getDataSource().releaseResource( redisResource );
        }
    }
    
    @Override
    protected void doRollback()
    {
        try
        {
            super.doRollback();
            transaction.discard();
        }
        finally
        {
            getDataSource().releaseResource( redisResource );
        }
    }
}
