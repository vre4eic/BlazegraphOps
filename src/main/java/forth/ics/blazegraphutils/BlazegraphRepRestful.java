package forth.ics.blazegraphutils;

import static forth.ics.blazegraphutils.BlazegraphRepLocal.loadProperties;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.client.ClientProtocolException;
import org.json.JSONObject;
import org.json.XML;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.rio.RDFFormat;

import java.util.concurrent.Future;

/**
 * Client usage sample for the Blazegraph Restful Service
 * 
* @author Vangelis Kritsotakis
 */
import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

public class BlazegraphRepRestful {

    private String serviceUrl;
    private Client clientPool;

    public BlazegraphRepRestful(String serviceUrl) throws IOException {
        this.serviceUrl = serviceUrl;
    }

    public BlazegraphRepRestful(String serviceUrl, Client clientPool) throws IOException {
        this.serviceUrl = serviceUrl;
        this.clientPool = clientPool;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public Client getClientPool() {
        return clientPool;
    }

    public void setClientPool(Client clientPool) {
        this.clientPool = clientPool;
    }

    /**
     * Imports an RDF like file on the server using post synchronously
     *
     * @param file A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format	The RDF format
     * @param namespace A String representation of the nameSpace
     * @param namedGraph A String representation of the nameGraph
     * @return A response from the service.
     */
    public Response importFile(String file, RDFFormat format, String namespace, String namedGraph)
            throws ClientProtocolException, IOException {
        String restURL = serviceUrl + "/namespace/" + namespace;// + "/sparql?context-uri=" + nameGraph
        // Taking into account nameSpace in the construction of the URL
        if (namespace != null) {
            restURL = serviceUrl + "/namespace/" + namespace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }
        // Taking into account nameGraph in the construction of the URL
        if (namedGraph != null) {
            restURL = restURL + "?context-uri=" + namedGraph;
        }
        String mimeType = fetchDataImportMimeType(format);
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL).queryParam("context-uri", namedGraph);
        Response response = webTarget.request().post(Entity.entity(new File(file), mimeType));// .form(form));
        return response;
    }

    /**
     * Imports an RDF like file on the server using post asynchronously
     *
     * @param file A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format	The RDF format
     * @param namespace A String representation of the nameSpace
     * @param namedGraph A String representation of the nameGraph
     * @return A response from the service.
     */
    public void importFileAsync(String file, RDFFormat format, String nameSpace, String nameGraph)
            throws ClientProtocolException, IOException, InterruptedException, ExecutionException {

        String responseStr = "";

        String restURL = serviceUrl + "/namespace/" + nameSpace;// + "/sparql?context-uri=" + nameGraph

        // Taking into account nameSpace in the construction of the URL
        if (nameSpace != null) {
            restURL = serviceUrl + "/namespace/" + nameSpace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }

        // Taking into account nameGraph in the construction of the URL
        if (nameGraph != null) {
            restURL = restURL + "?context-uri=" + nameGraph;
        }

        System.out.println(restURL);

        WebTarget webTarget = clientPool.target(restURL).queryParam("context-uri", nameGraph);

        AsyncInvoker asyncInvoker = webTarget.request().async();
        String mimeType = fetchDataImportMimeType(format);

        final Future<String> entityFuture = asyncInvoker.post(Entity.entity(new File(file), mimeType),
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String response) {
                        System.out.println("Response entity '" + response + "' received.");
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        System.out.println("Invocation failed.");
                        throwable.printStackTrace();
                    }

                });

    }

    // This is still in test 
    public String restPostBulkImportRDF() throws ClientProtocolException, IOException {

        String responseStr = "";

        //serviceUrl = http://139.91.183.88:9999/blazegraph
        String restURL = serviceUrl + "/dataloader";
        System.out.println(restURL);

        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL);

        Response response = webTarget.request().post(Entity.entity(new File("C:/Workspaces/vre4eicWorkspace/blazegraph-sesame-local/src/main/resources/bulkload.xml"), MediaType.APPLICATION_XML));
        responseStr = response.readEntity(String.class);

        System.out.println(response);

        return responseStr;

    }

    public void deleteNamespace(String namespace) throws Exception {
        if (namespaceExists(namespace)) {
            RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
            repository.deleteRepository(namespace);
            repository.close();
        }
    }

    public void createNamespace(String propFile, String namespace) throws Exception {
        Properties properties = loadProperties(propFile);
        if (!namespaceExists(namespace)) {
            RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
            repository.createRepository(namespace, properties);
            repository.close();
        }
    }

    private boolean namespaceExists(String namespace) throws Exception {
        RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
        GraphQueryResult res = repository.getRepositoryDescriptions();
        try {
            while (res.hasNext()) {
                Statement stmt = res.next();
                if (stmt.getPredicate()
                        .toString()
                        .equals(SD.KB_NAMESPACE.stringValue())) {
                    if (namespace.equals(stmt.getObject().stringValue())) {
                        return true;
                    }
                }
            }
        } finally {
            res.close();
            repository.close();
        }
        return false;
    }

    public Response clearGraphContents(String graph, String namespace) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("c", URLEncoder.encode("<" + graph + ">", "UTF-8").replaceAll("\\+", "%20"));
        Invocation.Builder invocationBuilder = webTarget.request();
        Response response = invocationBuilder.delete();
        return response;
    }

    public long importFolder(String folder, RDFFormat format, String namespace, String namedgraph) throws Exception {
        Response response;
        long duration = 0;
        for (File file : new File(folder).listFiles()) {
            if (!file.isDirectory()) {
                System.out.print("file: " + file.getName() + " .... in ");
                response = importFile(file.getAbsolutePath(), format, namespace, namedgraph);
                JSONObject json = XML.toJSONObject(response.readEntity(String.class));
                long curDur = ((JSONObject) json.get("data")).getLong("milliseconds");
                duration += curDur;
                System.out.println(curDur + " ms");
            }
        }
        return duration;
    }

    /**
     * Executes a SPARQL query.
     *
     * @param queryStr A String that holds the query to be submitted on the
     * server.
     * @param namespace A String representation of the nameSpace to be used
     * @param format
     * @return The output of the query
     * @throws java.io.UnsupportedEncodingException
     */
    public Response executeSparqlQuery(String queryStr, String namespace, QueryResultFormat format) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", URLEncoder.encode(queryStr, "UTF-8").replaceAll("\\+", "%20"));
        String mimetype = fetchQueryResultMimeType(format);
        Invocation.Builder invocationBuilder = webTarget.request(mimetype);
        Response response = invocationBuilder.get();
        return response;
    }

    /**
     * Executes synchronously an update based on query
     *
     * @param queryStr A String that holds the query to be submitted on the
     * server.
     * @param namespace A String representation of the nameSpace to be used
     * @return The response of the update request
     */
    public Response executeUpdateSparqlQuery(String queryStr, String namespace) throws UnsupportedEncodingException {
        String restURL = serviceUrl + "/namespace/" + namespace;
        // Taking into account nameSpace in the construction of the URL
        if (namespace != null) {
            restURL = serviceUrl + "/namespace/" + namespace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }
        System.out.println(restURL);
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL);
        String contentType = "application/sparql-update";
        Invocation.Builder invocationBuilder = webTarget.request(contentType);
        Response response = invocationBuilder.post(Entity.entity(queryStr, contentType));
        return response;
    }

    /**
     * Executes asynchronously an update based on query
     *
     * @param queryStr A String that holds the query to be submitted on the
     * server.
     * @param namespace A String representation of the nameSpace to be used
     * @return The response of the update request
     */
    public void executeAsyncUpdateSparqlQuery(String queryStr, String namespace) throws UnsupportedEncodingException {
        String restURL = serviceUrl + "/namespace/" + namespace;
        // Taking into account nameSpace in the construction of the URL
        if (namespace != null) {
            restURL = serviceUrl + "/namespace/" + namespace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }
        System.out.println(restURL);
        WebTarget webTarget = clientPool.target(restURL);
        AsyncInvoker asyncInvoker = webTarget.request().async();
        String contentType = "application/sparql-update";
        final Future<String> entityFuture = asyncInvoker.post(Entity.entity(queryStr, contentType),
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String response) {
                        System.out.println("Response entity '" + response + "' received.");
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        System.out.println("Invocation failed.");
                        throwable.printStackTrace();
                    }

                });

    }

    public long triplesNum(String graph, String namespace) throws UnsupportedEncodingException {
        String query = "select (count(*) as ?count) from <" + graph + "> where {?s ?p ?o}";
        Response response = executeSparqlQuery(query, namespace, QueryResultFormat.JSON);
        JSONObject json = new JSONObject(response.readEntity(String.class));
        JSONObject count = (JSONObject) json.getJSONObject("results").getJSONArray("bindings").get(0);
        return count.getJSONObject("count").getLong("value");
    }

    public long countSparqlResults(String query, String namespace) throws Exception {
        String queryTmp = query.toLowerCase();
        int end = queryTmp.indexOf("from");
        if (end == -1) {
            end = queryTmp.indexOf("where");
        }
        int start = queryTmp.indexOf(" ");
        StringBuilder sb = new StringBuilder();
        sb.append(query.substring(0, start)).append(" (count(*) as ?count) ").append(query.substring(end));
        Response response = executeSparqlQuery(sb.toString(), namespace, QueryResultFormat.JSON);
        JSONObject json = new JSONObject(response.readEntity(String.class));
        JSONObject count = (JSONObject) json.getJSONObject("results").getJSONArray("bindings").get(0);
        return count.getJSONObject("count").getLong("value");
    }

    private String fetchDataImportMimeType(RDFFormat format) {
        String mimeType;
        if (format == RDFFormat.RDFXML) {
            mimeType = "application/rdf+xml";
        } else if (format == RDFFormat.N3) {
            mimeType = "text/rdf+n3";
        } else if (format == RDFFormat.NTRIPLES) {
            mimeType = "text/plain";
        } else if (format == RDFFormat.TURTLE) {
            mimeType = "application/x-turtle";
        } else if (format == RDFFormat.JSONLD) {
            mimeType = "application/ld+json";
        } else if (format == RDFFormat.TRIG) {
            mimeType = "application/x-trig";
        } else if (format == RDFFormat.NQUADS) {
            mimeType = "text/x-nquads";
        } else {
            mimeType = null;
        }
        return mimeType;
    }

    private String fetchQueryResultMimeType(QueryResultFormat format) {
        String mimetype = "";
        switch (format) {
            case CSV:
                mimetype = "text/csv";
                break;
            case JSON:
                mimetype = "application/json";
                break;
            case TSV:
                mimetype = "text/tab-separated-values";
                break;
            case XML:
                mimetype = "application/sparql-results+xml";
                break;
        }
        return mimetype;
    }

}
