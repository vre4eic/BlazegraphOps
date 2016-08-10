package forth.ics.blazegraphutils;

/**
 * Client usage sample for the Blazegraph Restful Service
 * 
* @author Vangelis Kritsotakis
 */
import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import static forth.ics.blazegraphutils.BlazegraphRepLocal.loadProperties;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.json.JSONObject;
import org.json.XML;

import org.apache.http.client.ClientProtocolException;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.rio.RDFFormat;

public class BlazegraphRepRestful {

    //private RemoteRepositoryManager repository;
    private String serviceUrl;

    public BlazegraphRepRestful(String serviceUrl) throws IOException {
        this.serviceUrl = serviceUrl;
    }

    /**
     * Imports an RDF like file on the server
     *
     * @param file A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format
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

    public Response clearGraphContent(String graph, String namespace) throws UnsupportedEncodingException {
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
                response = importFile(file.getAbsolutePath(), format, namespace, namedgraph);
                JSONObject json = XML.toJSONObject(response.readEntity(String.class));
                long curDur = ((JSONObject) json.get("data")).getLong("milliseconds");
                duration += curDur;
            }
        }
        return duration;
    }

    /**
     * Imports an RDF-like file on the server
     *
     * @param queryStr A String that holds the query to be submitted on the
     * server.
     * @param namespace A String representation of the nameSpace to be used
     * @param format
     * @return The output of the query
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
