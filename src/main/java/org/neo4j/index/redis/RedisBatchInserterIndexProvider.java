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

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.redis.RedisIndexImplementation.SERVICE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.index.IndexStore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisBatchInserterIndexProvider implements BatchInserterIndexProvider
{
    private final BatchInserter inserter;
    private final IndexStore indexStore;
    private final JedisPool db;
    private final Map<IndexIdentifier, BatchInserterIndex> indexes =
        new HashMap<IndexIdentifier, BatchInserterIndex>();

    public RedisBatchInserterIndexProvider( BatchInserter inserter, Map<String, String> params )
    {
        this.inserter = inserter;
        this.indexStore = ((BatchInserterImpl) inserter).getIndexStore();
        this.db = RedisDataSource.newJedisPool( params );
    }
    
    @Override
    public BatchInserterIndex nodeIndex( String indexName, Map<String, String> config )
    {
        config = config( Node.class, indexName, config );
        return index( new IndexIdentifier( Node.class, indexName ), config );
    }

    @Override
    public BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        config = config( Relationship.class, indexName, config );
        return index( new IndexIdentifier( Relationship.class, indexName ), config );
    }
    
    protected Jedis newResource()
    {
        return db.getResource();
    }

    @Override
    public void shutdown()
    {
        db.destroy();
    }

    private BatchInserterIndex index( IndexIdentifier identifier, Map<String, String> config )
    {
        BatchInserterIndex index = indexes.get( identifier );
        if ( index == null )
        {
            index = new RedisBatchInserterIndex( this, inserter, identifier, config );
            indexes.put( identifier, index );
        }
        return index;
    }
    
    private Map<String, String> config( Class<? extends PropertyContainer> cls,
            String indexName, Map<String, String> config )
    {
        if ( config != null )
        {
            config = stringMap( new HashMap<String, String>( config ), PROVIDER, SERVICE_NAME );
            indexStore.setIfNecessary( cls, indexName, config );
            return config;
        }
        else
        {
            return indexStore.get( cls, indexName );
        }
    }
}
