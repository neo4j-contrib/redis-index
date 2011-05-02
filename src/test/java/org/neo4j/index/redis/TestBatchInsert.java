package org.neo4j.index.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.redis.RedisIndexImplementation.MULTIPLE_VALUES;

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
            BatchInserterIndex index = provider.nodeIndex( indexName, MULTIPLE_VALUES );
            for ( int i = 0; i < 1000000; i++ )
            {
                Map<String, Object> properties = MapUtil.map( "name", "Node " + i );
                long node = inserter.createNode( properties );
                index.add( node, properties );
                if ( i == 55555 ) testId = node;
                if ( i%100000 == 0 ) System.out.println( i );
            }
            
            int resultCount = 0;
            long t = System.currentTimeMillis();
            for ( int i = 0; i < 1000; i++ )
            {
                if ( index.get( "name", "Node " + i*500 ).getSingle() != null )
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
