package forth.ics.blazegraphutils.tests;

import forth.ics.blazegraphutils.BlazegraphRepRestful;
import forth.ics.blazegraphutils.QueryResultFormat;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.openrdf.rio.RDFFormat;

public class VkritsTest {

    public static void main(String[] args) throws Exception {

        String propFile = "/config/quads.properties";
        String service = "http://83.212.97.61:9999/blazegraph";
        Client clientPool = new ResteasyClientBuilder().connectionPoolSize(10).build();
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);

        // ************** Import Synchronously
//        Response importResponse = blaze.importFile(
//				"C:/Workspaces/vre4eicWorkspace/blazegraph-sesame-local/src/main/resources/EFO - 2.68.owl", // file
//				RDFFormat.RDFXML, // content type
//				"testnamespace", // namespace
//				"http://efo/268"); // nameGraph
//        System.out.println("--- Trying to import from file ---");
//        System.out.println("Status: " + importResponse.getStatus() + " " + importResponse.getStatusInfo());
//        System.out.println(importResponse.readEntity(String.class));
        // ************** Import Asynchronously
        // Initiating connection pool for async
        /*
         blaze.importFileAsync(
         "C:/Workspaces/vre4eicWorkspace/blazegraph-sesame-local/src/main/resources/EFO - 2.68.owl", // file
         RDFFormat.RDFXML, // content type
         "testnamespace", // namespace
         "http://efo/268"); // nameGraph
         */
        // Note that connection pool will not close as there is not 
        // any way for it to close now and thus the code will never terminate
        // ************** Update Synchronously
//        String updateQueryString = "delete where { graph <http://efo/268> { <http://purl.obolibrary.org/obo/BFO_0000050> ?p ?o } }";
//        Response updateResponse = blaze.executeUpdateSparqlQuery(updateQueryString, "testnamespace");
//        System.out.println();
//        System.out.println("--- Trying to update based on query ---");
//        System.out.println("Query: " + updateQueryString);
//        System.out.println("Status: " + updateResponse.getStatus() + " " + updateResponse.getStatusInfo());
//        System.out.println(updateResponse.readEntity(String.class));

        /*
         // ************** Update Asynchronously
        
         String updateQueryString = "delete where { graph <http://efo/268> { <http://purl.obolibrary.org/obo/BFO_0000050> ?p ?o } }";
         blaze.executeAsyncUpdateSparqlQuery(updateQueryString, "testnamespace");
         */
        int runs = 1000;
        String graph = "http://lifewatchgreece.com";
        for (int i = 0; i < runs; i++) {
//            Response response = graphDB.executeSparqlQueryRest("select * from <" + graph + "> where {?s ?p ?o} ", QueryResultFormat.JSON);
            blaze.executeSparqlQuery("select * from <" + graph + "> where {?s ?p ?o}", "test1", QueryResultFormat.JSON);
            System.out.println(i);
//            System.out.println(response.readEntity(String.class));
//            System.gc();
        }

    }

}
