package org.lyon1;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.lyon1.Listener;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.util.Map;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
@Path("/registerHandler")
public class RegisterHandler {

    private static final Logger log = LoggerFactory.getLogger(RegisterHandler.class);
    private final DatabaseManagementService dbms;
    private final Log logger;
    private final ObjectMapper objectMapper;

    public RegisterHandler(@Context DatabaseManagementService dbms, @Context Log logger) {
        this.dbms = dbms;

        this.logger = logger;
        this.objectMapper = new ObjectMapper();
    }

    @POST
    @Path("/register")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response runQuery(Map<String, String> body) {
        logger.info("chiamata");
        Listener listener = new Listener(this.logger);
        logger.info(listener.toString());

        final GraphDatabaseService graphDb = dbms.database("neo4j");
        logger.info(graphDb.toString());

        this.dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);

        logger.info("boh");
        return Response.ok().build();

    }


}
