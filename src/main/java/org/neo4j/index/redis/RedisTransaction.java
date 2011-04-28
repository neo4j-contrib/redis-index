/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;

import org.neo4j.index.base.AbstractCommand;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.KeyValueTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import redis.clients.jedis.Transaction;

class RedisTransaction extends KeyValueTransaction
{
    private Map<IndexIdentifier, Transaction> txs;
    
    RedisTransaction( int identifier, XaLogicalLog xaLog,
        RedisDataSource dataSource )
    {
        super( identifier, xaLog, dataSource );
    }
    
    @Override
    protected void doPrepare() throws XAException
    {
        super.doPrepare();
        RedisDataSource dataSource = (RedisDataSource) getDataSource();
//        dataSource.getWriteLock();
        txs = new HashMap<IndexIdentifier, Transaction>();
        try
        {
            for ( Map.Entry<IndexIdentifier, Collection<AbstractCommand>> entry : getCommands().entrySet() )
            {
                IndexIdentifier identifier = entry.getKey();
                TxDataBoth txData = getTxData( identifier );
                
                // TODO start a MULTI and push the commands to redis
                Transaction tx = dataSource.beginTx();
                txs.put( identifier, tx );
            }
            closeTxData();
        }
        finally
        {
//            dataSource.releaseWriteLock();
        }
    }
    
    @Override
    protected void doCommit()
    {
        // TODO COMMIT in redis
        for ( Transaction tx : txs.values() )
        {
            // TODO analyze result?
            tx.exec();
        }
    }
    
    @Override
    protected void doRollback()
    {
        super.doRollback();
        // TODO ROLLBACK in redis
        for ( Transaction tx : txs.values() )
        {
            tx.discard();
        }
    }
}
