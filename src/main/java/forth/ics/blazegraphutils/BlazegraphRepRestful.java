package forth.ics.blazegraphutils;

/**
 * Client usage sample for the Blazegraph Restful Service
 * 
* @author Vangelis Kritsotakis
 */
import static forth.ics.blazegraphutils.BlazegraphRepLocal.loadProperties;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;

public class BlazegraphRepRestful {

    //private RemoteRepositoryManager repository;
    private String serviceUrl;
    private Properties properties;

    public BlazegraphRepRestful(String propFile, String serviceUrl) throws IOException {
        this.serviceUrl = serviceUrl;
        // repository = new RemoteRepositoryManager(serviceUrl, false);
        properties = loadProperties(propFile);
    }

    /**
     * Imports an RDF like file on the server
     *
     * @param file A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format
     * @param nameSpace A String representation of the nameSpace
     * @param namedGraph A String representation of the nameGraph
     * @return A response from the service.
     */
    public Response importFile(String file, RDFFormat format, String nameSpace, String namedGraph)
            throws ClientProtocolException, IOException {
        String restURL = serviceUrl + "/namespace/" + nameSpace;// + "/sparql?context-uri=" + nameGraph
        // Taking into account nameSpace in the construction of the URL
        if (nameSpace != null) {
            restURL = serviceUrl + "/namespace/" + nameSpace + "/sparql";
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

    public void importDatasetTest(String filename, RDFFormat format, String graph, String namespace, int runs) throws Exception {
        long duration = 0;
        System.out.println("-- " + namespace + " --");
        Response response;
        for (int i = 0; i < runs; i++) {
            BlazegraphRepRemote blaze = new BlazegraphRepRemote(properties, serviceUrl);
            blaze.deleteNamespace(namespace);
            blaze.createNamespace(namespace);
            blaze.terminate();
            long curDur = 0;
            if (new File(filename).isDirectory()) {
                curDur = importFolder(filename, format, namespace, graph);
            } else {
                response = importFile(filename, format, namespace, graph);
                JSONObject json = XML.toJSONObject(response.readEntity(String.class));
                curDur = ((JSONObject) json.get("data")).getLong("milliseconds");
            }
            System.out.println(curDur);
            duration += curDur;
        }
        System.out.println(graph + ": " + triplesNum(graph, namespace) + "\t\t\tDuration: " + duration / runs);
        System.out.println("----");
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

    public static void main(String[] args) throws Exception {
        //BlazegraphRepRestful blaze = new BlazegraphRepRestful("/config/quads.properties", "http://139.91.183.88:9999/blazegraph/sparql");
        BlazegraphRepRestful blaze = new BlazegraphRepRestful("/config/quads.properties", "http://83.212.97.61:9999/blazegraph");
        int runs = 10;
//        blaze.importDatasetTest("C:/RdfData/cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, "http://cidoc/3.2.1", "cidoc-3_2_1", runs);
//        blaze.importDatasetTest("C:/RdfData/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo/2.48", "efo-2_48", runs);
//        blaze.importDatasetTest("C:/RdfData/EFO - 2.68.owl", RDFFormat.RDFXML, "http://efo/2.68", "efo-2_68", runs);
//        blaze.importDatasetTest("C:/RdfData/EFO - 2.691.owl", RDFFormat.RDFXML, "http://efo/2.691", "efo-2_691", runs);
//        blaze.importDatasetTest("C:/RdfData/Worms", RDFFormat.TURTLE, "http://worms", "worms", runs);
//        blaze.importDatasetTest("C:/RdfData/Fishbase", RDFFormat.TURTLE, "http://fishbase", "fishbase", runs);
//        blaze.importDatasetTest("C:/RdfData/Lifewatch", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", "lifewatch_large", 1);
        //synthetic data
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/01. very small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/vsmall", "lifewatch_very_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/02. small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/small", "lifewatch_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/03. med-small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medsmall", "lifewatch_medium_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/04. med-large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medlarge", "lifewatch_medium_large", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/05. large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", "lifewatch_large", runs);
//        long start = System.currentTimeMillis();
//        JSONObject xmlJSONObj = XML.toJSONObject(response.readEntity(String.class));
//        System.out.println(xmlJSONObj);
//        System.out.println(System.currentTimeMillis() - start);
        String namespace = "lifewatch_large";
        for (File file : new File("C:\\Dropbox\\Shared Netbeans Projects\\Forth Projects\\VirtuosoOps\\input\\LifeWatchGreece_Queries").listFiles()) {
            String query = BlazegraphRepRemote.readData(file.getAbsolutePath());
            long start = System.currentTimeMillis();
//            blaze.executeSparqlQuery(query, namespace, QueryResultFormat.JSON);
            blaze.countSparqlResults(query, namespace);
            System.out.println(file.getName() + "\t" + (System.currentTimeMillis() - start) + "\t" + blaze.countSparqlResults(query, namespace));
        }

        ///
//        response = blaze.executeSparqlQuery("select * from <http://cidoc/test> where {?s ?p ?o}", "resttest");
//        System.out.println(response.readEntity(String.class));
//        blaze.triplesNum("http://cidoc/test", "resttest");
        //String queryOutput = blaze.restGetSubmitQuery2("select  * from <http://cidoc/test> where {?s ?p ?o }", "testnamespace");
        //System.out.println(queryOutput);
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
