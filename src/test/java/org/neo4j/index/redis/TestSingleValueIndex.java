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
import static org.junit.Assert.assertNull;
import static org.neo4j.index.redis.Neo4jTestCase.deleteFileOrDirectory;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestSingleValueIndex
{
    private static final String PATH = "target/var/one-to-one";
    private static final Map<String, String> REDIS_CONFIG = RedisIndexImplementation.SINGLE_VALUE;
    private GraphDatabaseService db;
    private Transaction tx;
    
    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( new File( PATH ) );
        db = new EmbeddedGraphDatabase( PATH );
    }
    
    @After
    public void doAfter()
    {
        db.shutdown();
    }
    
    private void beginTx()
    {
        tx = db.beginTx();
    }
    
    private void finishTx( boolean success )
    {
        if ( success )
        {
            tx.success();
        }
        tx.finish();
    }
    
    private void restartTx()
    {
        finishTx( true );
        beginTx();
    }
    
    private Index<Node> nodeIndex( String name )
    {
        // Create/delete it because of the nature of redis... it's a server
        // and its indexes are there even after a test and we've cleared the db
        Transaction tx = db.beginTx();
        try
        {
            Index<Node> index = db.index().forNodes( name, REDIS_CONFIG );
            index.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return db.index().forNodes( name, REDIS_CONFIG );
    }
    
    @Test
    public void basic()
    {
        Index<Node> index = nodeIndex( "basic" );
        String key = "key";
        
        beginTx();
        Node a = db.createNode();
        Node b = db.createNode();
        index.add( a, key, "1" );
        index.add( b, key, "2" );
        assertEquals( a, index.get( key, "1" ).getSingle() );
        assertEquals( b, index.get( key, "2" ).getSingle() );
        finishTx( true );
        assertEquals( a, index.get( key, "1" ).getSingle() );
        assertEquals( b, index.get( key, "2" ).getSingle() );
        
        beginTx();
        index.remove( b, key, "2" );
        assertEquals( a, index.get( key, "1" ).getSingle() );
        assertNull( index.get( key, "2" ).getSingle() );
        finishTx( true );
        assertEquals( a, index.get( key, "1" ).getSingle() );
        assertNull( index.get( key, "2" ).getSingle() );
    }
    
    @Ignore
    @Test
    public void canOnlyHaveOneValueEntityPerKeyValue() throws Exception
    {
        Index<Node> index = nodeIndex( "one-entity" );
        String key = "key";
        String value = "value";
        beginTx();
        Node nodeA = db.createNode();
        Node nodeB = db.createNode();
        finishTx( true );
        
        // Transactional state only
        beginTx();
        assertNull( index.get( key, value ).getSingle() );
        index.add( nodeA, key, value );
        assertEquals( nodeA, index.get( key, value ).getSingle() );
        index.add( nodeB, key, value );
        assertEquals( nodeB, index.get( key, value ).getSingle() );
        finishTx( false );
        
        // Mixed tx state and committed
        beginTx();
        assertNull( index.get( key, value ).getSingle() );
        index.add( nodeA, key, value );
        assertEquals( nodeA, index.get( key, value ).getSingle() );
        restartTx();
        assertEquals( nodeA, index.get( key, value ).getSingle() );
        index.add( nodeB, key, value );
        assertEquals( nodeB, index.get( key, value ).getSingle() );
        restartTx();
        assertEquals( nodeB, index.get( key, value ).getSingle() );
        finishTx( true );
        
        beginTx();
        index.remove( nodeB, key, "whatever" );
        assertEquals( nodeB, index.get( key, value ).getSingle() );
        index.remove( nodeB, key, value );
        assertNull( index.get( key, value ).getSingle() );
        finishTx( true );
        assertNull( index.get( key, value ).getSingle() );
    }

    @Ignore
    @Test
    public void testInsertionSpeed()
    {
        Index<Node> index = nodeIndex( "speed" );
        beginTx();
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 10000000; i++ )
        {
            Node entity = db.createNode();
            index.add( entity, "name", i );
            if ( i % 30000 == 0 )
            {
                restartTx();
                System.out.println( i );
            }
        }
        finishTx( true );
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );

        t = System.currentTimeMillis();
        int count = 100000;
        int resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            for ( Node entity : index.get( "name", i*300 ) )
            {
                resultCount++;
            }
        }
        System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );
    }
}
