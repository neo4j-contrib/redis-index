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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.base.IndexDataSource;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.keyvalue.KeyValueCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * An {@link XaDataSource} optimized for the {@link LuceneIndexProvider}.
 * This class is public because the XA framework requires it.
 */
public class RedisDataSource extends IndexDataSource
{
    /**
     * The normal delimiter between parts in a redis key, f.ex:
     * indexName:key:value
     */
    static final char KEY_DELIMITER = ':';
    
    /**
     * Delimiter to use between a "normal" part and a part that represents
     * an entity ({@link Node}/{@link Relationship}) id just to avoid collitions
     * with keys only consisting of normal parts.
     */
    static final char ID_DELIMITER = '|';
    static final String NAME = "redis";
    static final byte[] BRANCH_ID = "redis".getBytes();
    
    private JedisPool db;
    
    /**
     * Constructs this data source.
     * 
     * @param params XA parameters.
     * @throws InstantiationException if the data source couldn't be
     * instantiated
     */
    public RedisDataSource( Map<Object,Object> params ) 
        throws InstantiationException
    {
        super( params );
    }
    
    @Override
    protected void initializeBeforeLogicalLog( Map<?, ?> params )
    {
        // TODO read config somehow... not just "localhost"
        GenericObjectPool.Config jedisPoolConfig = new GenericObjectPool.Config();
        db = new JedisPool( jedisPoolConfig, "localhost" );
    }

    @Override
    protected void actualClose()
    {
        // TODO shutdown redis
        db.destroy();
    }
    
    protected XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new RedisTransaction( identifier, logicalLog, this );
    }
    
    @Override
    protected XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer )
            throws IOException
    {
        return KeyValueCommand.readCommand( channel, buffer, this );
    }

    @Override
    protected void flushAll()
    {
        // TODO
    }

    public Jedis acquireResource()
    {
        return db.getResource();
    }
    
    public void releaseResource( Jedis resource )
    {
        db.returnResource( resource );
    }
    
    private StringBuilder redisKeyStart( IndexIdentifier identifier )
    {
        char entityType = identifier.getEntityType().equals( Node.class ) ? 'n' : 'r';
        return new StringBuilder( entityType ).append( KEY_DELIMITER ).append( identifier.getIndexName() );
    }

    public String formRedisKeyForKeyValue( IndexIdentifier identifier, String key, String value )
    {
        return redisKeyStart( identifier ).append( KEY_DELIMITER ).append( key ).append( KEY_DELIMITER )
                .append( value ).toString();
    }
    
    public String formRedisKeyForEntityAndKeyRemoval( IndexIdentifier identifier, String key, long id )
    {
        return redisKeyStart( identifier ).append( KEY_DELIMITER )
                .append( key ).append( ID_DELIMITER ).append( id ).toString();
    }
    
    public String formRedisKeyForEntityRemoval( IndexIdentifier identifier, long id )
    {
        return redisKeyStart( identifier ).append( ID_DELIMITER ).append( id ).toString();
    }
    
    public String formRedisStartNodeKey( IndexIdentifier identifier, long id)
    {
        return redisKeyStart( identifier ).append(KEY_DELIMITER)
                .append("start").append(ID_DELIMITER).append(id).toString();
    }

    public String formRedisEndNodeKey(IndexIdentifier identifier, long id)
    {
        return redisKeyStart( identifier ).append(KEY_DELIMITER)
                .append("end").append(ID_DELIMITER).append(id).toString();
    }
}
