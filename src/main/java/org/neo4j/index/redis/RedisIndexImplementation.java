package org.neo4j.index.redis;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.base.AbstractIndexImplementation;
import org.neo4j.index.base.IndexDataSource;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.kernel.Config;

public class RedisIndexImplementation extends AbstractIndexImplementation
{
    public static final String SERVICE_NAME = "redis";
    
    public RedisIndexImplementation( GraphDatabaseService db, Config config )
    {
        super( db, config );
    }
    
    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        return new RedisIndex.NodeIndex( this, new IndexIdentifier( Node.class, indexName ) );
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config )
    {
        return new RedisIndex.RelationshipIndex( this, new IndexIdentifier( Relationship.class, indexName ) );
    }
    
    @Override
    protected Class<? extends IndexDataSource> getDataSourceClass()
    {
        return RedisDataSource.class;
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return config;
    }

    @Override
    public String getDataSourceName()
    {
        return RedisDataSource.NAME;
    }

    @Override
    protected byte[] getDataSourceBranchId()
    {
        return RedisDataSource.BRANCH_ID;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig, Map<String, String> config )
    {
        return true;
    }
}
