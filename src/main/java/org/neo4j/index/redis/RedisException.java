package org.neo4j.index.redis;

public class RedisException extends RuntimeException
{
    public RedisException( Throwable cause )
    {
        super( cause );
    }

    public RedisException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
