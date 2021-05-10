package com.maxdemarzi;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.concurrent.Phaser;

public class NextHop implements Runnable {
    private final GraphDatabaseService db;
    private KernelTransaction ktx;
    private final Log log;
    private final Roaring64NavigableMap next;
    private final Roaring64NavigableMap current;
    private final int[] types;
    private Phaser ph;

    public NextHop(GraphDatabaseService db, KernelTransaction ktx, Log log, Roaring64NavigableMap next,
                   Roaring64NavigableMap current, int[] types, Phaser ph) {
        this.db = db;
        this.ktx = ktx;
        this.log = log;
        this.next = next;
        this.current = current;
        this.types = types;
        this.ph = ph;
        ph.register();
    }

    @Override
    public void run() {
        try(Transaction tx = db.beginTx()) {
            CursorFactory cursors = ktx.cursors();
            Read read = ktx.dataRead();

            RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor(ktx.pageCursorTracer());
            NodeCursor nodeCursor = cursors.allocateNodeCursor(ktx.pageCursorTracer());

            current.forEach(l -> {
                read.singleNode(l,nodeCursor);
                nodeCursor.next();

                if (types.length == 0) {
                    nodeCursor.relationships(rels, RelationshipSelection.ALL_RELATIONSHIPS);
                    while (rels.next()) {
                        next.add(rels.otherNodeReference());
                    }
                } else {
                    RelationshipTraversalCursor typedRels = RelationshipSelections.allCursor(cursors, nodeCursor, types, ktx.pageCursorTracer());
                    while (typedRels.next()) {
                        next.add(typedRels.otherNodeReference());
                    }
                }
            });
        }
        ph.arriveAndDeregister();
    }
}
