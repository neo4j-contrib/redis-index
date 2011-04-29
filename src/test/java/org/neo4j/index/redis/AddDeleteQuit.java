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

import static org.neo4j.index.redis.Neo4jTestCase.nodeIndex;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * This class is used by {@link TestRecovery} so that a graph database can
 * be shut down in a non-clean way after index add, then index delete.
 */
public class AddDeleteQuit
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( args[0] );
        Index<Node> index = nodeIndex( db, "index" );
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            index.add( node, "key", "value" );
            index.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.exit( 0 );
    }
}
