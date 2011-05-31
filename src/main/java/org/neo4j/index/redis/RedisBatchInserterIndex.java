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

import static org.neo4j.index.redis.RedisDataSource.formRedisKeyForKeyValue;
import static org.neo4j.index.redis.RedisDataSource.getIndexType;

import java.util.Map;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.NoIndexHits;
import org.neo4j.index.base.SingleIndexHit;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisBatchInserterIndex implements BatchInserterIndex
{
    private static final int PIPELINE_EXECUTE_THRESHOLD = 10000;
    private final RedisBatchInserterIndexProvider provider;
    private final BatchInserter inserter;
    private final IndexIdentifier identifier;
    private final Map<String, String> config;
    private final IndexType indexType;
    private final boolean includeRelationshipInformation;
    
    private Jedis writeResource;
    private Pipeline pipeline;
    private Jedis readResource;
    private int pipelineSize;

    public RedisBatchInserterIndex( RedisBatchInserterIndexProvider provider,
            BatchInserter inserter, IndexIdentifier identifier, Map<String, String> config )
    {
        this.provider = provider;
        this.inserter = inserter;
        this.identifier = identifier;
        this.config = config;
        this.indexType = getIndexType( config );
        this.includeRelationshipInformation = identifier.getEntityType().equals( Relationship.class ) &&
                indexType == IndexType.multiple_values;
        
        writeResource = provider.newResource();
        newPipeline();
        readResource = provider.newResource();
    }

    private void newPipeline()
    {
        pipeline = writeResource.pipelined();
        pipeline.multi();
    }
    
    @Override
    public void add( long entityId, Map<String, Object> properties )
    {
        for ( Map.Entry<String, Object> property : properties.entrySet() )
        {
            // TODO Get rid of this if-statement, via inheritance of whatever
            long startNode = -1L;
            long endNode = -1L;
            if ( includeRelationshipInformation )
            {
                SimpleRelationship rel = inserter.getRelationshipById( entityId );
                startNode = rel.getStartNode();
                endNode = rel.getEndNode();
            }
            indexType.add( pipeline, identifier, property.getKey(), property.getValue().toString(),
                    entityId, startNode, endNode );
            pipelineSize++;
        }
        checkPipelineThreshold();
    }

    private void checkPipelineThreshold()
    {
        if ( pipelineSize > PIPELINE_EXECUTE_THRESHOLD )
        {
            restartPipeline();
            pipelineSize = 0;
        }
    }

    @Override
    public void updateOrAdd( long entityId, Map<String, Object> properties )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexHits<Long> get( String key, Object value )
    {
        String idString = readResource.get( formRedisKeyForKeyValue( identifier, key, value.toString() ) );
        if ( idString != null && !idString.equals( "nil" ) )
        {
            return new SingleIndexHit<Long>( Long.valueOf( idString ) );
        }
        return NoIndexHits.instance();
    }

    @Override
    public IndexHits<Long> query( String key, Object queryOrQueryObject )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexHits<Long> query( Object queryOrQueryObject )
    {
        throw new UnsupportedOperationException();
    }
    
    void close()
    {
        execPipeline();
    }

    @Override
    public void flush()
    {
        restartPipeline();
    }

    private void restartPipeline()
    {
        execPipeline();
        newPipeline();
    }

    private void execPipeline()
    {
        pipeline.exec();
        pipeline.execute();
        pipeline = null;
        pipelineSize = 0;
    }

    @Override
    public void setCacheCapacity( String key, int size )
    {
        throw new UnsupportedOperationException();
    }
}
