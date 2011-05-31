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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.redis.RedisIndexImplementation.SINGLE_VALUE;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class TestBatchInsert
{
    @Test
    public void makeSureBatchInsertedIndexCanBePickedUpInNormalMode()
    {
        String path = "target/var/batch";
        String indexName = "index";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        BatchInserter inserter = new BatchInserterImpl( path );
        BatchInserterIndexProvider provider = new RedisBatchInserterIndexProvider( inserter, stringMap() );
        long testId = 0;
        try
        {
            BatchInserterIndex index = provider.nodeIndex( indexName, SINGLE_VALUE );
            for ( int i = 0; i < 100000; i++ )
            {
                Map<String, Object> properties = MapUtil.map( "name", "Node " + i );
                long node = inserter.createNode( properties );
                index.add( node, properties );
                if ( i == 55555 ) testId = node;
            }
            
            int resultCount = 0;
            long t = System.currentTimeMillis();
            for ( int i = 0; i < 1000; i++ )
            {
                if ( index.get( "name", "Node " + i*50 ).getSingle() != null )
                {
                    resultCount++;
                }
            }
            t = System.currentTimeMillis()-t;
            System.out.println( "Get " + resultCount + ", " + t + "ms" + " (" + ((double)t/(double)resultCount) + ")" );
        }
        finally
        {
            provider.shutdown();
            inserter.shutdown();
        }
        
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        try
        {
            Index<Node> realIndex = db.index().forNodes( indexName );
            Node node = realIndex.get( "name", "Node " + 55555 ).getSingle();
            assertNotNull( node );
            assertEquals( testId, node.getId() );
            db.shutdown();
        }
        finally
        {
            db.shutdown();
        }
    }
}
