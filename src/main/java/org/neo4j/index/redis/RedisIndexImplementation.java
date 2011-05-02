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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.base.AbstractIndexImplementation;
import org.neo4j.index.base.IndexDataSource;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.kernel.Config;

public class RedisIndexImplementation extends AbstractIndexImplementation
{
    public static final String SERVICE_NAME = "redis";
    static final String DEFAULT_INDEX_TYPE = IndexType.single_value.name();
    static final String CONFIG_KEY_TYPE = "type";
    
    public static final Map<String, String> SINGLE_VALUE = MapUtil.stringMap(
            IndexManager.PROVIDER, SERVICE_NAME, CONFIG_KEY_TYPE, IndexType.single_value.name() );
    public static final Map<String, String> MULTIPLE_VALUES = MapUtil.stringMap(
            IndexManager.PROVIDER, SERVICE_NAME, CONFIG_KEY_TYPE, IndexType.multiple_values.name() );
    
    public RedisIndexImplementation( GraphDatabaseService db, Config config )
    {
        super( db, config );
    }
    
    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        IndexIdentifier identifier = new IndexIdentifier( Node.class, indexName );
        IndexType type = dataSource().getIndexType( identifier );
        switch ( type )
        {
        case multiple_values: return new RedisIndex.NodeIndex( this, identifier );
        case single_value: return new RedisSingleValueIndex.NodeIndex( this, identifier );
        default: throw new IllegalArgumentException( "" + type );
        }
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config )
    {
        IndexIdentifier identifier = new IndexIdentifier( Relationship.class, indexName );
        IndexType type = dataSource().getIndexType( identifier );
        switch ( type )
        {
        case multiple_values: return new RedisIndex.RelationshipIndex( this, identifier );
        case single_value: return new RedisSingleValueIndex.RelationshipIndex( this, identifier );
        default: throw new IllegalArgumentException( "" + type );
        }
    }
    
    @Override
    protected Class<? extends IndexDataSource> getDataSourceClass()
    {
        return RedisDataSource.class;
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        String type = config.get( CONFIG_KEY_TYPE );
        if ( type == null )
        {
            config.put( CONFIG_KEY_TYPE, DEFAULT_INDEX_TYPE );
        }
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
        String storedType = storedConfig.get( CONFIG_KEY_TYPE );
        String customType = config.get( CONFIG_KEY_TYPE );
        return storedType.equals( customType != null ? customType : DEFAULT_INDEX_TYPE );
    }
    
    @Override
    public RedisDataSource dataSource()
    {
        return (RedisDataSource) super.dataSource();
    }
}
