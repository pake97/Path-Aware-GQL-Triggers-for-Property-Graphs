package org.lyon1;



import static com.google.common.collect.Iterables.size;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.Log;



public class Listener implements TransactionEventListener<CreatedEntitiesCounter>
{

    private Log logger;

    public Listener(Log logger){

        this.logger = logger;
    }

    @Override
    public CreatedEntitiesCounter beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) throws Exception {
        Log logger = this.logger;
        logger.info("Transaction Before Commit");


        Map<String, Object> metaData = data.metaData();



        logger.info("Before Metadata:");
        metaData.forEach((k, v) -> logger.info("  {}: {}", k, v));

        logger.info("Before Created Nodes:");
        for (Node node : data.createdNodes()) {
            String node_id = node.getElementId().split(":")[2];
            logger.info(node.getElementId());
            logger.info(node_id);
            Iterable<Label> labels = node.getLabels();
            for (Label l : labels){
                logger.info("label" +  l.name());
            }
            Map<String,Object> properties = node.getAllProperties();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                logger.info(entry.getKey() + " = " + entry.getValue());
            }
        }



        logger.info("Before Created Relationships:");
        for (Relationship rel : data.createdRelationships()) {
            logger.info("  Relationship ID: {}, Type: {}, Start: {}, End: {}, Properties: {}",
                    rel.getElementId(), rel.getType().name(),
                    rel.getStartNode().getElementId(), rel.getEndNode().getElementId(),
                    rel.getAllProperties());
        }

        logger.info("Before Deleted Nodes:");
        for (Node node : data.deletedNodes()) {
            logger.info("  Node ID: {}", node.getElementId());
        }

        logger.info("Before Deleted Relationships:");
        for (Relationship rel : data.deletedRelationships()) {
            logger.info("  Relationship ID: {}", rel.getElementId());
        }

        logger.info("Before Assigned Labels:");
        for (LabelEntry entry : data.assignedLabels()) {
            logger.info("  Node ID: {}, Label: {}", entry.node().getElementId(), entry.label().name());
        }

        logger.info("Before Removed Labels:");
        for (LabelEntry entry : data.removedLabels()) {
            logger.info("  Node ID: {}, Label: {}", entry.node().getElementId(), entry.label().name());
        }

        logger.info("Before Assigned Node Properties:");
        for (PropertyEntry<Node> entry : data.assignedNodeProperties()) {
            logger.info("  Node ID: {}, Key: {}, Value: {}", entry.entity().getElementId(), entry.key(), entry.value());
        }

        logger.info("Before Removed Node Properties:");
        for (PropertyEntry<Node> entry : data.removedNodeProperties()) {
            logger.info("  Node ID: {}, Key: {}, Value: {}", entry.entity().getElementId(), entry.key(), entry.value());
        }

        logger.info("Before Assigned Relationship Properties:");
        for (PropertyEntry<Relationship> entry : data.assignedRelationshipProperties()) {
            logger.info("  Relationship ID: {}, Key: {}, Value: {}", entry.entity().getElementId(), entry.key(), entry.value());
        }

        logger.info("Before Removed Relationship Properties:");
        for (PropertyEntry<Relationship> entry : data.removedRelationshipProperties()) {
            logger.info("  Relationship ID: {}, Key: {}, Value: {}", entry.entity().getElementId(), entry.key(), entry.value());
        }

        return null;
    }

    @Override
    public void afterCommit( TransactionData data, CreatedEntitiesCounter entitiesCounter, GraphDatabaseService databaseService ) {
        Log logger = this.logger;
        logger.info(data.getTransactionId() + "");
        logger.info("After Created Nodes:");

        for (Node node : data.createdNodes()) {
            String node_id = node.getElementId();
            try (Transaction tx = databaseService.beginTx();
                 Result result = tx.execute("MATCH (n) WHERE elementId(n)='" + node_id + "' RETURN n, labels(n) AS labels, keys(n) AS keys")) {
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    logger.info(row.get("n").toString());
                }
                tx.commit();
            }
        }

        for (Relationship relationship : data.createdRelationships()) {
            String rel_id = relationship.getElementId();
            try (Transaction tx = databaseService.beginTx();
                 Result result = tx.execute(String.format("MATCH ()-[r:RE]->() WHERE elementID(r) = '%s' RETURN r, type(r), keys(r)", rel_id))) {
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    logger.info(row.get("r").toString());
                }
                tx.commit();
            }
        }

        for (PropertyEntry<Node> propertyEntry : data.assignedNodeProperties()) {
            String nodeId = propertyEntry.entity().getElementId();
            String propertyKey = propertyEntry.key();
            Object propertyValue = propertyEntry.value();
            try (Transaction tx = databaseService.beginTx();
                 Result result = tx.execute(String.format(
                         "MATCH (n) WHERE elementID(n) = '%s' RETURN n.%s AS propertyValue, n, keys(n)",
                         nodeId, propertyKey))) {
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    logger.info(String.format("Node: %s, Property %s = %s", nodeId, propertyKey, row.get("propertyValue")));
                }
                tx.commit();
            }
        }




    }
//
//
//    @Override
//    public void afterCommit( TransactionData data, CreatedEntitiesCounter entitiesCounter, GraphDatabaseService databaseService )
//    {
//        Log logger = this.logger;
//        logger.info("Transaction after commit");
//
//        logger.info("After Created Nodes:");
//        for (Node node : data.createdNodes()) {
//            logger.info(node.getElementId());
//            Iterable<Label> labels = node.getLabels();
//            for (Label l : labels) {
//                logger.info("label" + l.name());
//            }
//            Map<String, Object> properties = node.getAllProperties();
//            for (Map.Entry<String, Object> entry : properties.entrySet()) {
//                logger.info(entry.getKey() + " = " + entry.getValue());
//            }
//        }
//
//
//
//    }



    @Override
    public void afterRollback( TransactionData data, CreatedEntitiesCounter state, GraphDatabaseService databaseService )
    {
        logger.info("Transaction  after rollback");
    }



}


class CreatedEntitiesCounter
{
    private final long createdNodes;
    private final long createdRelationships;

    public CreatedEntitiesCounter( long createdNodes, long createdRelationships )
    {
        this.createdNodes = createdNodes;
        this.createdRelationships = createdRelationships;
    }

    public long getCreatedNodes()
    {
        return createdNodes;
    }

    public long getCreatedRelationships()
    {
        return createdRelationships;
    }
}
