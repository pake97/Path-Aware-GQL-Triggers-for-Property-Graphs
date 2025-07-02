package org.lyon1;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

@Path("/intercept")
public class QueryInterceptor {

    private static final Logger log = LoggerFactory.getLogger(QueryInterceptor.class);
    private final DatabaseManagementService dbms;
    private final Log logger;
    private final ObjectMapper objectMapper;

    public QueryInterceptor(@Context DatabaseManagementService dbms, @Context Log logger) {
        this.dbms = dbms;
        this.logger = logger;
        this.objectMapper = new ObjectMapper();
    }

    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response runQuery(Map<String, String> body) {
        String cypher = body.get("query");

        logger.info("Received query: " + cypher);

        if (cypher == null || cypher.trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Missing or empty query string"))
                    .build();
        }

        if (cypher.toLowerCase().contains("delete")) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(Map.of("error", "DELETE queries not allowed"))
                    .build();
        }

        StreamingOutput stream = os -> {
            JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
            jg.writeStartObject();
            jg.writeFieldName("results");
            jg.writeStartArray();

            final GraphDatabaseService graphDb = dbms.database("neo4j");
            try (Transaction tx = graphDb.beginTx(); Result result = tx.execute(cypher)) {
                while (result.hasNext()) {
                    logger.info("1");
                    Map<String, Object> row = result.next();
                    jg.writeStartObject(); // Each row = {
                    logger.info("2");
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        jg.writeFieldName(entry.getKey());
                        logger.info("3");
                        Object value = entry.getValue();
                        if (value instanceof Node node) {
                            jg.writeStartObject();
                            jg.writeStringField("type", "node");
                            jg.writeStringField("id", node.getElementId());
                            logger.info("4");
                            jg.writeFieldName("labels");
                            jg.writeStartArray();
                            for (Label label : node.getLabels()) {
                                jg.writeString(label.toString());
                                logger.info("5");
                            }
                            jg.writeEndArray();

                            jg.writeFieldName("properties");
                            jg.writeObject(node.getAllProperties());

                            jg.writeEndObject();
                        } else if (value instanceof Relationship rel) {
                            logger.info("6");
                            jg.writeStartObject();
                            jg.writeStringField("type", "relationship");
                            jg.writeStringField("id", rel.getElementId());
                            jg.writeStringField("relType", rel.getType().name());
                            jg.writeStringField("startNodeId", rel.getStartNode().getElementId());
                            jg.writeStringField("endNodeId", rel.getEndNode().getElementId());

                            jg.writeFieldName("properties");
                            jg.writeObject(rel.getAllProperties());

                            jg.writeEndObject();
                        }
                    }
                    jg.writeEndObject();
                }
                logger.info("7");
//                         else if (value instanceof Path path) {
//                            jg.writeStartObject();
//                            jg.writeStringField("type", "path");
//                            jg.writeFieldName("nodes");
//                            jg.writeStartArray();
//                            for (Node n : path)     {
//                                jg.writeStartObject();
//                                jg.writeNumberField("id", n.getId());
//
//                                jg.writeFieldName("labels");
//                                jg.writeStartArray();
//                                for (String label : n.getLabels()) {
//                                    jg.writeString(label);
//                                }
//                                jg.writeEndArray();
//
//                                jg.writeFieldName("properties");
//                                jg.writeObject(n.getAllProperties());
//                                jg.writeEndObject();
//                            }
                        //}

                logger.info("8");
                tx.commit();
            } catch (Exception e) {
                logger.error("Query execution failed", e);
                jg.writeEndArray();
                jg.writeFieldName("error");
                jg.writeString(e.getMessage());
                jg.writeEndObject();
                jg.flush();
                jg.close();
                return;
            }
            logger.info("9");
            jg.writeEndArray();
            logger.info("10");
            jg.writeEndObject();
            logger.info("11");
            jg.flush();
            logger.info("12");
            jg.close();
            logger.info("13");
        };

        logger.info("14");
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

    }
}
