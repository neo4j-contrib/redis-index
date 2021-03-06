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

import org.apache.commons.pool.impl.GenericObjectPool;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.base.IndexDataSource;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.ParamsUtil;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * An {@link XaDataSource} optimized for the {@link RedisIndexProvider}.
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

    static final String REDIS_PREFIX = "index.redis.";
    static final String REDIS_POOL_PREFIX = REDIS_PREFIX + "pool.";
    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 6379;
    static final int DEFAULT_TIMEOUT = 2000;
    static final int DEFAULT_DATABASE = 0;

    private JedisPool db;

    // the target Redis database numeric index
    // needs to be static because newJedisPool is called from a static context in the batch inserter
    private static int targetDatabase;

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
    protected void initializeBeforeLogicalLog( Map<?, ?> params ) {

        db = newJedisPool( params );
        //TODO check that redis is accessible. For the moment tests through NPE if it's not
    }
    
    static JedisPool newJedisPool( Map<?, ?> params )
    {
        // jedis parameters
        String host = ParamsUtil.getString(params, REDIS_PREFIX + "host", DEFAULT_HOST);
        Integer port = ParamsUtil.getInt(params, REDIS_PREFIX + "port", DEFAULT_PORT);
        Integer timeout = ParamsUtil.getInt(params, REDIS_PREFIX + "timeout", DEFAULT_TIMEOUT);
        String password = ParamsUtil.getString(params, REDIS_PREFIX + "password");

        // selected database
        targetDatabase = ParamsUtil.getInt(params, REDIS_PREFIX + "database", DEFAULT_DATABASE);

        // connection pool parameters
        Integer poolMaxIdle = ParamsUtil.getInt(params, REDIS_POOL_PREFIX + "maxIdle");
        Integer poolMinIdle = ParamsUtil.getInt(params, REDIS_POOL_PREFIX + "minIdle");
        Integer poolMaxActive = ParamsUtil.getInt(params, REDIS_POOL_PREFIX + "maxActive");
        Long poolMaxWait = ParamsUtil.getLong(params, REDIS_POOL_PREFIX + "maxWait");

        GenericObjectPool.Config jedisPoolConfig = new GenericObjectPool.Config();
        if (poolMaxIdle != null) {
            jedisPoolConfig.maxIdle = poolMaxIdle;
        }
        if (poolMinIdle != null) {
            jedisPoolConfig.minIdle = poolMinIdle;
        }
        if (poolMaxActive != null) {
            jedisPoolConfig.maxActive = poolMaxActive;
        }
        if (poolMaxWait != null) {
            jedisPoolConfig.maxWait = poolMaxWait;
        }

        return new JedisPool(jedisPoolConfig, host, port, timeout, password);
    }

    @Override
    protected void actualClose()
    {
        db.destroy();
    }

    protected XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new RedisTransaction( identifier, logicalLog, this );
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

    public static int getTargetDatabase()
    {
        return targetDatabase;
    }

    private static StringBuilder redisKeyStart( IndexIdentifier identifier )
    {
        String entityType = identifier.getEntityType().equals( Node.class ) ? "n" : "r";
        return new StringBuilder( entityType ).append(KEY_DELIMITER).append(identifier.getIndexName());
    }

    public static String formRedisKeyForIndex( IndexIdentifier identifier )
    {
        return redisKeyStart(identifier).toString();
    }
    
    public static String formRedisKeyForKeyValue( IndexIdentifier identifier, String key, String value )
    {
        return redisKeyStart( identifier ).append( KEY_DELIMITER ).append( key ).append( KEY_DELIMITER )
                .append( value ).toString();
    }

    public static String formRedisKeyForEntityAndKeyRemoval( IndexIdentifier identifier, String key, long id )
    {
        return redisKeyStart( identifier ).append( KEY_DELIMITER )
                .append(key).append( ID_DELIMITER ).append( id ).toString();
    }

    public static String formRedisKeyForEntityRemoval( IndexIdentifier identifier, long id )
    {
        return redisKeyStart(identifier).append( ID_DELIMITER ).append( id ).toString();
    }

    public static String formRedisStartNodeKey( IndexIdentifier identifier, long id)
    {
        return redisKeyStart( identifier ).append(KEY_DELIMITER)
                .append("start").append(ID_DELIMITER).append(id).toString();
    }

    public static String formRedisEndNodeKey(IndexIdentifier identifier, long id)
    {
        return redisKeyStart( identifier ).append(KEY_DELIMITER)
                .append("end").append(ID_DELIMITER).append(id).toString();
    }

    // pattern to look up all the keys related to an index using the Redis "key" command
    public String formRedisIndexPattern ( IndexIdentifier identifier )
    {
        return redisKeyStart( identifier ).append('[')
                .append(KEY_DELIMITER).append(ID_DELIMITER).append("]*").toString() ;
    }
    
    public IndexType getIndexType( IndexIdentifier identifier )
    {
        // TODO optimize... by a cache maybe?
        Map<String, String> config = getIndexStore().get( identifier.getEntityType(), identifier.getIndexName() );
        return getIndexType(config);
    }
    
    public static IndexType getIndexType( Map<String, String> config )
    {
        return IndexType.valueOf( config.get( "type" ) );
    }
}
