package forth.ics.blazegraphutils;

import javax.ws.rs.client.Client;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.openrdf.rio.RDFFormat;

public class VkritsTest {

	public static void main(String[] args) throws Exception {
		
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.88:9999/blazegraph";
        Client clientPool = new ResteasyClientBuilder().connectionPoolSize(10).build();
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service, clientPool);
        
		// Asynchronously
		
		// Initiating connection pool for async
		blaze.importFileAsync(
				"C:/Workspaces/vre4eicWorkspace/blazegraph-sesame-local/src/main/resources/EFO - 2.68.owl", // file
				RDFFormat.RDFXML, // content type
				"testnamespace", // namespace
				"http://efo/268"); // nameGraph
		
		// Note that connection pool will not close as there is not 
		// any way for it to close now and thus the code will never terminate
		
		
	}
	
	
}
