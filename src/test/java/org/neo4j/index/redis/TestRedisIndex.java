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

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.redis.Contains.contains;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestRedisIndex
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;

    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/db";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }
    
    @After
    public void commitTx()
    {
        finishTx( true );
    }
    
    private void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.finish();
            tx = null;
        }
    }

//    @Before
    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        commitTx();
        beginTx();
    }

    private static abstract interface EntityCreator<T extends PropertyContainer>
    {
        T create( Object... properties );
        
        void delete( T entity );
    }

    private static final RelationshipType TEST_TYPE =
            DynamicRelationshipType.withName( "TEST_TYPE" );
    private static final EntityCreator<Node> NODE_CREATOR = new EntityCreator<Node>()
    {
        public Node create( Object... properties )
        {
            Node node = graphDb.createNode();
            setProperties( node, properties );
            return node;
        }
        
        public void delete( Node entity )
        {
            entity.delete();
        }
    };
    private static final EntityCreator<Relationship> RELATIONSHIP_CREATOR =
            new EntityCreator<Relationship>()
            {
                public Relationship create( Object... properties )
                {
                    Relationship rel = graphDb.createNode().createRelationshipTo( graphDb.createNode(), TEST_TYPE );
                    setProperties( rel, properties );
                    return rel;
                }
                
                public void delete( Relationship entity )
                {
                    entity.delete();
                }
            };

    static class FastRelationshipCreator implements EntityCreator<Relationship>
    {
        private Node node, otherNode;

        public Relationship create( Object... properties )
        {
            if ( node == null )
            {
                node = graphDb.createNode();
                otherNode = graphDb.createNode();
            }
            Relationship rel = node.createRelationshipTo( otherNode, TEST_TYPE );
            setProperties( rel, properties );
            return rel;
        }
        
        public void delete( Relationship entity )
        {
            entity.delete();
        }
    }
    
    private static void setProperties( PropertyContainer entity, Object... properties )
    {
        for ( Map.Entry<String, Object> entry : MapUtil.map( properties ).entrySet() )
        {
            entity.setProperty( entry.getKey(), entry.getValue() );
        }
    }
    
    private <T extends PropertyContainer> void makeSureAdditionsCanBeRead(
            Index<T> index, EntityCreator<T> entityCreator )
    {
        beginTx();
        String key = "name";
        String value = "Mattias";
        assertThat( index.get( key, value ).getSingle(), is( nullValue() ) );
        assertThat( index.get( key, value ), Contains.<T>contains() );

        T entity1 = entityCreator.create();
        T entity2 = entityCreator.create();
        index.add( entity1, key, value );
        assertThat( index.get( key, value ), contains( entity1 ) );
        assertThat( index.get( key, value ), contains( entity1 ) );

        restartTx();
        assertThat( index.get( key, value ), contains( entity1 ) );
        assertThat( index.get( key, value ), contains( entity1 ) );

        index.add( entity2, key, value );
        assertThat( index.get( key, value ), contains( entity1, entity2 ) );

        restartTx();
        assertThat( index.get( key, value ), contains( entity1, entity2 ) );
        index.delete();
    }
    
    private Index<Node> nodeIndex( String name )
    {
        return Neo4jTestCase.nodeIndex( graphDb, name );
    }
    
    private RelationshipIndex relationshipIndex( String name )
    {
        return Neo4jTestCase.relIndex( graphDb, name );
    }

    @Test
    public void makeSureAdditionsCanBeReadNodeExact()
    {
        makeSureAdditionsCanBeRead( nodeIndex( "additions-node" ), NODE_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadRelationshipExact()
    {
        makeSureAdditionsCanBeRead( relationshipIndex( "additions-rel" ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeRemovedInSameTx()
    {
        makeSureAdditionsCanBeRemoved( false );
    }
    
    @Test
    public void makeSureYouCanAskIfAnIndexExistsOrNot()
    {
        String name = "index-that-may-exist";
        assertFalse( graphDb.index().existsForNodes( name ) );
        nodeIndex( name );
        beginTx();
        assertTrue( graphDb.index().existsForNodes( name ) );

        assertFalse( graphDb.index().existsForRelationships( name ) );
        relationshipIndex( name );
        assertTrue( graphDb.index().existsForRelationships( name ) );
    }

    private void makeSureAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = nodeIndex( "some-index" + restartTx );
        beginTx();
        String key = "name";
        String value = "Mattias";
        assertNull( index.get( key, value ).getSingle() );
        Node node = graphDb.createNode();
        index.add( node, key, value );
        if ( restartTx )
        {
            restartTx();
        }
        assertEquals( node, index.get( key, value ).getSingle() );
        index.remove( node, key, value );
        assertNull( index.get( key, value ).getSingle() );
        restartTx();
        assertNull( index.get( key, value ).getSingle() );
        node.delete();
        index.delete();
    }

    @Test
    public void makeSureAdditionsCanBeRemoved()
    {
        makeSureAdditionsCanBeRemoved( true );
    }

    private void makeSureSomeAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = nodeIndex( "some-index-2-" + restartTx );
        beginTx();
        String key1 = "name";
        String key2 = "title";
        String value1 = "Mattias";
        assertNull( index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        Node node = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node, key1, value1 );
        index.add( node, key2, value1 );
        index.add( node2, key1, value1 );
        if ( restartTx )
        {
            restartTx();
        }
        index.remove( node, key1, value1 );
        index.remove( node, key2, value1 );
        assertEquals( node2, index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        assertEquals( node2, index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        node.delete();
        index.delete();
    }

    @Test
    public void makeSureSomeAdditionsCanBeRemovedInSameTx()
    {
        makeSureSomeAdditionsCanBeRemoved( false );
    }

    @Test
    public void makeSureSomeAdditionsCanBeRemoved()
    {
        makeSureSomeAdditionsCanBeRemoved( true );
    }

    @Test
    public void makeSureThereCanBeMoreThanOneValueForAKeyAndEntity()
    {
        makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( false );
    }

    @Test
    public void makeSureThereCanBeMoreThanOneValueForAKeyAndEntitySameTx()
    {
        makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( true );
    }

    private void makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( boolean restartTx )
    {
        Index<Node> index = nodeIndex( "many-values-" + restartTx );
        beginTx();
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        Node node = graphDb.createNode();
        index.add( node, key, value1 );
        index.add( node, key, value2 );
        if ( restartTx )
        {
            restartTx();
        }
        index.add( node, key, value3 );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), Contains.<Node>contains() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), Contains.<Node>contains() );
        index.delete();
    }

    @Test
    public void makeSureArrayValuesAreSupported()
    {
        Index<Node> index = nodeIndex( "arrays" );
        beginTx();
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        Node node = graphDb.createNode();
        index.add( node, key, new String[]{value1, value2, value3} );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), Contains.<Node>contains() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), Contains.<Node>contains() );

        index.remove( node, key, new String[]{value2, value3} );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), Contains.<Node>contains() );
        assertThat( index.get( key, value3 ), Contains.<Node>contains() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), Contains.<Node>contains() );
        assertThat( index.get( key, value3 ), Contains.<Node>contains() );
        index.delete();
    }

    private <T extends PropertyContainer> void doSomeRandomUseCaseTestingWithExactIndex(
            Index<T> index, EntityCreator<T> creator )
    {
        beginTx();
        String name = "name";
        String mattias = "Mattias Persson";
        String title = "title";
        String hacker = "Hacker";

        assertThat( index.get( name, mattias ), Contains.<T>contains() );

        T entity1 = creator.create();
        T entity2 = creator.create();

        assertNull( index.get( name, mattias ).getSingle() );
        index.add( entity1, name, mattias );
        assertThat( index.get( name, mattias ), contains( entity1 ) );

        assertEquals( entity1, index.get( name, mattias ).getSingle() );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertEquals( entity1, index.get( name, mattias ).getSingle() );

        beginTx();
        index.add( entity2, title, hacker );
        index.add( entity1, title, hacker );
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1, entity2 ) );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1, entity2 ) );

        beginTx();
        index.remove( entity2, title, hacker );
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1 ) );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1 ) );

        beginTx();
        index.remove( entity1, title, hacker );
        index.remove( entity1, name, mattias );
//        index.delete();
        commitTx();
    }

    @Test
    public void doSomeRandomUseCaseTestingWithNodeIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( nodeIndex( "usecase" ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomUseCaseTestingWithRelationshipIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( relationshipIndex( "usecase" ), RELATIONSHIP_CREATOR );
    }

    private <T extends PropertyContainer> void doSomeRandomTestingWithFulltextIndex(
            Index<T> index,
            EntityCreator<T> creator )
    {
        beginTx();
        T entity1 = creator.create();
        T entity2 = creator.create();

        String key = "name";
        index.add( entity1, key, "The quick brown fox" );
        index.add( entity2, key, "brown fox jumped over" );

        assertThat( index.get( key, "The quick brown fox" ), contains( entity1 ) );
        assertThat( index.get( key, "brown fox jumped over" ), contains( entity2 ) );
        assertThat( index.query( key, "quick" ), contains( entity1 ) );
        assertThat( index.query( key, "brown" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "quick OR jumped" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "brown AND fox" ), contains( entity1, entity2 ) );

        restartTx();
        
        assertThat( index.get( key, "The quick brown fox" ), contains( entity1 ) );
        assertThat( index.get( key, "brown fox jumped over" ), contains( entity2 ) );
        assertThat( index.query( key, "quick" ), contains( entity1 ) );
        assertThat( index.query( key, "brown" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "quick OR jumped" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "brown AND fox" ), contains( entity1, entity2 ) );

        index.delete();
    }

    private <T extends PropertyContainer> void testInsertionSpeed(
            Index<T> index, EntityCreator<T> creator )
    {
        beginTx();
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            T entity = creator.create();
            IteratorUtil.lastOrNull( (Iterable<T>) index.get( "name", "The name " + i ) );
            index.add( entity, "name", "The name " + i );
            index.add( entity, "title", "Some title " + i );
            index.add( entity, "something", i + "Nothing" );
            index.add( entity, "else", i + "kdfjkdjf" + i );
            if ( i % 10000 == 0 )
            {
                restartTx();
                System.out.println( i );
            }
        }
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );

        t = System.currentTimeMillis();
        int count = 1000;
        int resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            for ( T entity : index.get( "name", "The name " + i*900 ) )
            {
                resultCount++;
            }
        }
        System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );

        t = System.currentTimeMillis();
        resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            for ( T entity : index.get( "something", i*900 + "Nothing" ) )
            {
                resultCount++;
            }
        }
        System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );
    }
    
    @Ignore
    @Test
    public void testNodeInsertionSpeed()
    {
        testInsertionSpeed( nodeIndex( "insertion-speed" ), NODE_CREATOR );
    }

    @Ignore
    @Test
    public void testRelationshipInsertionSpeed()
    {
        testInsertionSpeed( relationshipIndex( "insertion-speed" ), new FastRelationshipCreator() );
    }
    
    @Test
    public void makeSureIndexNameAndConfigCanBeReachedFromIndex()
    {
        String indexName = "my-index-1";
        Index<Node> nodeIndex = nodeIndex( indexName );
        beginTx();
        assertEquals( indexName, nodeIndex.getName() );
        assertEquals( stringMap( "provider", RedisIndexImplementation.SERVICE_NAME ),
                graphDb.index().getConfiguration( nodeIndex ) );
    }
    
    @Test
    public void makeSureYouCanRemoveFromRelationshipIndex()
    {
        RelationshipIndex index = relationshipIndex( "rel-index" );
        beginTx();
        Node n1 = graphDb.createNode();
        Node n2 = graphDb.createNode();
        Relationship r = n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "foo" ) );
        String key = "bar";
        index.remove( r, key, "value" );
        index.add( r, key, "otherValue" );
        assertThat( index.get( key, "value" ), Contains.<Relationship>contains() );
        assertThat( index.get( key, "otherValue" ), contains( r ) );
        restartTx();
        assertThat( index.get( key, "value" ), Contains.<Relationship>contains() );
        assertThat( index.get( key, "otherValue" ), contains( r ) );
    }
    
    @Test
    public void makeSureYouCanGetEntityTypeFromIndex()
    {
        Index<Node> nodeIndex = nodeIndex( "type-test" );
        Index<Relationship> relIndex = relationshipIndex( "type-test" );
        beginTx();
        assertEquals( Node.class, nodeIndex.getEntityType() );
        assertEquals( Relationship.class, relIndex.getEntityType() );
    }
    
    @Test
    public void makeSureConfigurationCanBeModified()
    {
        Index<Node> index = nodeIndex( "conf-index" );
        try
        {
            graphDb.index().setConfiguration( index, "provider", "something" );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e ) { /* Good*/ }
        try
        {
            graphDb.index().removeConfiguration( index, "provider" );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e ) { /* Good*/ }

        beginTx();
        String key = "my-key";
        String value = "my-value";
        String newValue = "my-new-value";
        assertNull( graphDb.index().setConfiguration( index, key, value ) );
        assertEquals( value, graphDb.index().getConfiguration( index ).get( key ) );
        assertEquals( value, graphDb.index().setConfiguration( index, key, newValue ) );
        assertEquals( newValue, graphDb.index().getConfiguration( index ).get( key ) );
        assertEquals( newValue, graphDb.index().removeConfiguration( index, key ) );
        assertNull( graphDb.index().getConfiguration( index ).get( key ) );
    }

    @Test
    public void testSomeStuff() throws Exception
    {
        String indexName = "" + currentTimeMillis();
        Index<Node> index = graphDb.index().forNodes( indexName, stringMap( "provider", RedisIndexImplementation.SERVICE_NAME ) );
        
        beginTx();
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node1, "name", "Mattias" );
        restartTx();
        
        index.add( node2, "name", "Mattias" );
        assertThat( index.get( "name", "Mattias" ), contains( node1, node2 ) );
        restartTx();
        
        assertThat( index.get( "name", "Mattias" ), contains( node1, node2 ) );
        index.remove( node1, "name", "Mattias" );
        assertThat( index.get( "name", "Mattias" ), contains( node2 ) );
        restartTx();
        
        assertThat( index.get( "name", "Mattias" ), contains( node2 ) );
        index.remove( node2, "name", "Mattias" );
        assertThat( index.get( "name", "Mattias" ), Contains.<Node>contains() );
        node1.delete();
        node2.delete();
        finishTx( true );
    }
    
    @Test
    public void multipleIndexesSameTransaction() throws Exception
    {
        Index<Node> nodes = nodeIndex( "multi" );
        Index<Relationship> rels = relationshipIndex( "multi" );
        
        beginTx();
        Node from = graphDb.createNode();
        Node to = graphDb.createNode();
        Relationship rel = from.createRelationshipTo( to, TEST_TYPE );
        nodes.add( from, "name", "from" );
        nodes.add( to, "name", "to" );
        rels.add( rel, "type", rel.getType().name() );
        assertEquals( from, nodes.get( "name", "from" ).getSingle() );
        assertEquals( to, nodes.get( "name", "to" ).getSingle() );
        assertEquals( rel, rels.get( "type", TEST_TYPE.name() ).getSingle() );
        restartTx();
        assertEquals( from, nodes.get( "name", "from" ).getSingle() );
        assertEquals( to, nodes.get( "name", "to" ).getSingle() );
        assertEquals( rel, rels.get( "type", TEST_TYPE.name() ).getSingle() );
        
        nodes.delete();
        rels.delete();
    }

    @Test
    public void relationshipIndex() throws Exception
    {
        RelationshipIndex rels = relationshipIndex("relationships");
        beginTx();
        Node node1 = graphDb.createNode(),
        node2 = graphDb.createNode(),
        node3 = graphDb.createNode();

        Relationship relationship1 = node1.createRelationshipTo(node3, TEST_TYPE);
        Relationship relationship2 = node2.createRelationshipTo(node3, TEST_TYPE);

        String key = "key1";
        String value = "value1";

        rels.add(relationship1, key, value);
        rels.add(relationship2, key, value);
        commitTx();

        IndexHits<Relationship> hits = rels.get(key, value, node1, node3);
        assertEquals(1, hits.size());
        assertEquals(hits.getSingle().getId(), relationship1.getId());
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKey(
            EntityCreator<T> creator, Index<T> index ) throws Exception
    {
        String key1 = "key1";
        String key2 = "key2";
        String value = "value";
        
        beginTx();
        T entity1 = creator.create();
        index.add( entity1, key1, value );
        index.add( entity1, key2, value );
        T entity2 = creator.create();
        index.add( entity2, key1, value );
        index.add( entity2, key2, value );
        restartTx();
        
        assertThat( index.get( key1, value ), contains( entity1, entity2 ) );
        assertThat( index.get( key2, value ), contains( entity1, entity2 ) );
        index.remove( entity1, key2 );
        assertThat( index.get( key1, value ), contains( entity1, entity2 ) );
        assertThat( index.get( key2, value ), contains( entity2 ) );
        index.add( entity1, key2, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key1, value ), contains( entity1, entity2 ) );
            assertThat( index.get( key2, value ), contains( entity1, entity2 ) );
            restartTx();
        }
    }
    
    @Test
    public void testRemoveWithoutKeyNodes() throws Exception
    {
        testRemoveWithoutKey( NODE_CREATOR, nodeIndex( "remove-wo-k" ) ); 
    }
    
    @Test
    public void testRemoveWithoutKeyRelationships() throws Exception
    {
        testRemoveWithoutKey( RELATIONSHIP_CREATOR, relationshipIndex( "remove-wo-k" ) );
    }
    
    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKeyValue(
            EntityCreator<T> creator, Index<T> index ) throws Exception
    {
        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";
        
        beginTx();
        T entity1 = creator.create();
        index.add( entity1, key1, value1 );
        index.add( entity1, key2, value2 );
        T entity2 = creator.create();
        index.add( entity2, key1, value1 );
        index.add( entity2, key2, value2 );
        restartTx();
        
        assertThat( index.get( key1, value1 ), contains( entity1, entity2 ) );
        assertThat( index.get( key2, value2 ), contains( entity1, entity2 ) );
        index.remove( entity1 );
        assertThat( index.get( key1, value1 ), contains( entity2 ) );
        assertThat( index.get( key2, value2 ), contains( entity2 ) );
        index.add( entity1, key1, value1 );
        
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key1, value1 ), contains( entity1, entity2 ) );
            assertThat( index.get( key2, value2 ), contains( entity2 ) );
            restartTx();
        }
    }
    
    @Test
    public void testRemoveWithoutKeyValueNodes() throws Exception
    {
        testRemoveWithoutKeyValue( NODE_CREATOR, nodeIndex( "remove-wo-kv" ) ); 
    }
    
    @Test
    public void testRemoveWithoutKeyValueRelationships() throws Exception
    {
        testRemoveWithoutKeyValue( RELATIONSHIP_CREATOR, relationshipIndex( "remove-wo-kv" ) );
    }
    
    @Test
    public void nodeAndRelationshipIndexWithSameName() throws Exception
    {
        String indexName = "same-index";
        Index<Node> nodeIndex = nodeIndex( indexName );
        RelationshipIndex relIndex = relationshipIndex( indexName );
        
        beginTx();
        String key = "key";
        String value = "value";
        Node node = graphDb.createNode();
        Relationship rel = node.createRelationshipTo( graphDb.createNode(), TEST_TYPE );
        nodeIndex.add( node, key, value );
        relIndex.add( rel, key, value );
        
        assertThat( nodeIndex.get( key, value ), contains( node ) );
        assertThat( relIndex.get( key, value ), contains( rel ) );
        commitTx();
        assertThat( nodeIndex.get( key, value ), contains( node ) );
        assertThat( relIndex.get( key, value ), contains( rel ) );
    }
}
