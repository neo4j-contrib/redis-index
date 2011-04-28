package org.neo4j.index.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * @author Tareq Abedrabbo
 */
public class RedisIndexTest {

    private static GraphDatabaseService graphDb;
    private static IndexManager indexManager;
    private static Map<String, String> indexConfig = new HashMap<String, String>();
    private static String timestamp;

    @BeforeClass
    public static void init() {
        graphDb = new EmbeddedGraphDatabase("var/graphdb");
        indexManager = graphDb.index();
        indexConfig.put("provider", RedisIndexImplementation.SERVICE_NAME);
        timestamp = String.valueOf(new Date().getTime());
        System.out.println("timestamp:" + timestamp);
    }

    @Test
    public void addAndGet() throws Exception {
        Index<Node> index = indexManager.forNodes(timestamp + "_1", indexConfig);
        Transaction tx = graphDb.beginTx();
        Node node = null;
        try {
            node = graphDb.createNode();
            index.add(node, "timestamp", timestamp);
            tx.success();
        } finally {
            tx.finish();
        }

        IndexHits<Node> hits = index.get("timestamp", timestamp);
        assertNotNull( hits );

        Node result = hits.getSingle();
        assertNotNull( result );
        assertEquals( node, result );
    }

    @Test
    public void addMultiple() throws Exception {
        Index<Node> index = indexManager.forNodes(timestamp + "_2", indexConfig);
        Transaction tx = graphDb.beginTx();
        List<Node> nodes = new ArrayList<Node>();
        try {
            for (int i = 0; i < 5; i++) {
                Node node = graphDb.createNode();
                index.add(node, "timestamp", timestamp);
                nodes.add(node);

            }
            tx.success();
        } finally {
            tx.finish();
        }

        IndexHits<Node> hits = index.get("timestamp", timestamp);
        assertNotNull( hits );
        assertThat( hits, Contains.contains( nodes ) );
    }


    @AfterClass
    public static void shutdown() {
        graphDb.shutdown();
    }
}
