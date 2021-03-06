/* 
 * Copyright 2017 VRE4EIC Consortium
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.vre4eic.evre.blazegraph;

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
import java.io.BufferedInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.simple.parser.JSONParser;

public class BlazegraphRepRestful {

    private String serviceUrl;
    private Client clientPool;
    private String namespace;
    private int timeout;
    private boolean logging = true;

    public BlazegraphRepRestful(String serviceUrl) {
        this.timeout = 0;
        this.serviceUrl = serviceUrl;
    }

    public BlazegraphRepRestful(String serviceUrl, String namespace) {
        this.timeout = 0;
        this.serviceUrl = serviceUrl;
        this.namespace = namespace;
    }

    public BlazegraphRepRestful(String serviceUrl, Client clientPool) {
        this.timeout = 0;
        this.serviceUrl = serviceUrl;
        this.clientPool = clientPool;
    }

    private static Properties loadProperties(String resource) throws IOException {
        Properties p = new Properties();
        InputStream is = BlazegraphRepLocal.class
                .getResourceAsStream(resource);
        p.load(new InputStreamReader(new BufferedInputStream(is)));
        return p;
    }

    public void setLogging(boolean logging) {
        this.logging = logging;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getNamespace() {
        return namespace;
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
     * @param file: A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format: The RDF format {@link RDFFormat} of the file.
     * @param namespace: The namespace repository which will hold the data.
     * @param namedGraph: The named graph URI in which the data will be
     * inserted.
     * @return A response from the service.
     * @throws IOException
     */
    public String importFilePath(String file, RDFFormat format, String namespace, String namedGraph)
            throws IOException {
        String mimeType = Utils.fetchDataImportMimeType(format);
        return importFilePath(file, mimeType, namespace, namedGraph);
    }

    public String importFilePath(String file, RDFFormat format, String namedGraph)
            throws IOException {
        String mimeType = Utils.fetchDataImportMimeType(format);
        return importFilePath(file, mimeType, namespace, namedGraph);
    }

    /**
     * Imports an RDF like file on the server using post synchronously.
     *
     * @param file: A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param mimetypeFormat: The mimetype of the data which will be inserted.
     * The accepted mimetypes can be found in {
     * @
     * https://wiki.blazegraph.com/wiki/index.php/REST_API#MIME_Types}.
     * @param namespace: The namespace repository which will hold the data.
     * @param namedGraph: The named graph URI in which the data will be
     * inserted.
     * @return A response from the service.
     * @throws IOException
     */
    public String importFilePath(String file, String mimetypeFormat, String namespace, String namedGraph)
            throws IOException {
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
        if (logging) {
            System.out.println("Importing file: " + file + " into graph: " + namedGraph);
        }
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL).queryParam("context-uri", namedGraph);
        long start = System.currentTimeMillis();
        Response response = webTarget.request().post(Entity.entity(new File(file), mimetypeFormat));// .form(form));
        if (logging) {
            System.out.println("--- Duration: " + (System.currentTimeMillis() - start) + " ---");
        }
        if (response.getStatus() == 200) {
            return xmlImportToJson(response.readEntity(String.class));
        } else {
            return response.readEntity(String.class);
        }
    }

    public String importFilePath(String file, String mimetypeFormat, String namedGraph)
            throws IOException {
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
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL).queryParam("context-uri", namedGraph);
        Response response = webTarget.request().post(Entity.entity(new File(file), mimetypeFormat));// .form(form));
        if (response.getStatus() == 200) {
            return xmlImportToJson(response.readEntity(String.class));
        } else {
            return response.readEntity(String.class);
        }
    }

    /**
     * Imports an RDF data String in the server using post synchronously.
     *
     * @param fileContentStr: The String object which contains the RDF data.
     * @param mimetypeFormat: The mimetype of the data contained in the data
     * String. The accepted mimetypes can be found in
     * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#MIME_Types">https://wiki.blazegraph.com/wiki/index.php/REST_API#MIME_Types</a>.
     * @param namespace: The namespace repository which will hold the data.
     * @param namedGraph: The named graph URI in which the data will be
     * inserted.
     * @return A response from the service.
     * @throws IOException
     */
    public String importFileData(String fileContentStr, String mimetypeFormat, String namespace, String namedGraph)
            throws IOException {
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
        String mimeType = mimetypeFormat;
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL).queryParam("context-uri", namedGraph);
        Response response = webTarget.request().post(Entity.entity(fileContentStr, mimeType));// .form(form));
        int status = response.getStatus();
        if (status == 200) {
            return xmlImportToJson(response.readEntity(String.class));
        } else {
            return response.readEntity(String.class);
        }
    }

    private String xmlImportToJson(String xml) {
        JSONObject json = XML.toJSONObject(xml);
        return json.toString();
    }

    /**
     * Imports an RDF like file on the server using post asynchronously
     *
     * @param file: A String holding the path of the file, the contents of which
     * will be uploaded.
     * @param format: The RDF format of the file contents.
     * @param namespace: The namespace repository which will hold the data.
     * @param namedGraph: The named graph URI in which the data will be
     * inserted.
     * @return A response from the service.
     */
    public void importFileAsync(String file, RDFFormat format, String nameSpace, String nameGraph)
            throws ClientProtocolException, IOException, InterruptedException, ExecutionException {

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
        String mimeType = Utils.fetchDataImportMimeType(format);
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

    /**
     *
     * @param mimetype
     * @param namespace
     * @param graph
     * @return
     * @throws UnsupportedEncodingException
     */
    public Response exportFile(String mimetype, String namespace, String graph) throws UnsupportedEncodingException {
        if (mimetype == null) {
            mimetype = Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES);
        }
        Client client = ClientBuilder.newClient();
        StringBuilder sb = new StringBuilder();
        sb.append("CONSTRUCT { ?s ?p ?o } WHERE { ");
        if (graph != null) {
            sb.append("graph <" + graph + "> {");
        }
        sb.append(" hint:Query hint:constructDistinctSPO false . ?s ?p ?o } ");
        if (graph != null) {
            sb.append("}");
        }
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", URLEncoder.encode(sb.toString(), "UTF-8").replaceAll("\\+", "%20"));
        Invocation.Builder invocationBuilder = webTarget.request(mimetype);
        Response response = invocationBuilder.get();
        return response;
    }

    public Response exportFile(RDFFormat format, String namespace, String graph) throws UnsupportedEncodingException {
        String mimetype = Utils.fetchDataImportMimeType(format);
        return exportFile(mimetype, namespace, graph);
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

    /**
     * Deletes a namespace repository and all its contents from the Blazegraph
     * triplestore.
     *
     * @param namespace: The namespace name which will be deleted.
     * @return: True if a namespace with the given name exists, false otherwise.
     * @throws Exception
     */
    public boolean deleteNamespace(String namespace) throws Exception {
        if (namespaceExists(namespace)) {
            RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
            repository.deleteRepository(namespace);
            repository.close();
            return true;
        }
        return false;
    }

    /**
     * Creates a new namespace repository with a specific configuration given as
     * parameter. Information on the parameters of a Blazegraph properties file
     * can be found here:
     * <a href="https://wiki.blazegraph.com/wiki/index.php/Configuring_Blazegraph">https://wiki.blazegraph.com/wiki/index.php/Configuring_Blazegraph</a>.
     *
     * @param resourcePropFile: The file path the configuration properties file
     * located in the resources of the project.
     * @param namespace: The namespace name which will be created.
     * @return: true if there does not exist any namespace with the given name,
     * false otherwise.
     * @throws Exception
     */
    public boolean createNamespaceFromResources(String resourcePropFile, String namespace) throws Exception {
        Properties properties = loadProperties(resourcePropFile);
        //////////////
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, "com.bigdata.rdf.vocab.MyVocabularyVersion1");
//        properties.setProperty(AbstractTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG, "{\"config\":{\"uri\":\"http://my-lat-lon-starttime-endtime-dt\",\"fields\":[{\"valueType\":\"DOUBLE\",\"multiplier\":\"100000\",\"serviceMapping\":\"LATITUDE\"},{\"valueType\":\"DOUBLE\",\"multiplier\":\"100000\",\"serviceMapping\":\"LONGITUDE\"},{\"valueType\":\"LONG\",\"serviceMapping\":\"starttime\"},{\"valueType\":\"LONG\",\"serviceMapping\":\"endtime\"}]}}");
        if (!namespaceExists(namespace)) {
            RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
            repository.createRepository(namespace, properties);
            repository.close();
            return true;
        }
        return false;
    }

    public boolean createNamespaceFromFile(String propFile, String namespace) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileReader(propFile));
        if (!namespaceExists(namespace)) {
            RemoteRepositoryManager repository = new RemoteRepositoryManager(serviceUrl, false);
            repository.createRepository(namespace, properties);
            repository.close();
            return true;
        }
        return false;
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

    /**
     * Clears the contents of a named graph located in a specific namespace
     * repository.
     *
     * @param graph: The named graph URI whose contents will be deleted.
     * @param namespace: The namespace repository which contains the graph.
     * @return: A response from the service.
     * @throws UnsupportedEncodingException
     */
    public Response clearGraphContents(String graph, String namespace) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("c", URLEncoder.encode("<" + graph + ">", "UTF-8").replaceAll("\\+", "%20"));
        Invocation.Builder invocationBuilder = webTarget.request();
        Response response = invocationBuilder.delete();
        client.close();
        return response;
    }

    public Response clearGraphContents(String graph) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("c", URLEncoder.encode("<" + graph + ">", "UTF-8").replaceAll("\\+", "%20"));
        Invocation.Builder invocationBuilder = webTarget.request();
        Response response = invocationBuilder.delete();
        client.close();
        return response;
    }

    /**
     *
     * @param folder
     * @param mimetypeFormat
     * @param format
     * @param namespace
     * @param namedgraph
     * @return
     * @throws Exception
     */
    public long importFolder(String folder, String mimetypeFormat, String namespace, String namedgraph, String extFilter) {
        String responseString;
        long duration = 0;
        for (File file : new File(folder).listFiles()) {
            if (!file.isDirectory()) {
//                System.out.print("file: " + file.getName() + " .... in ");
                if (extFilter != null && !file.getName().endsWith(extFilter)) {
                    continue;
                }
                try {
                    responseString = importFilePath(file.getAbsolutePath(), mimetypeFormat, namespace, namedgraph);
                    JSONParser parser = new JSONParser();
                    org.json.simple.JSONObject json = (org.json.simple.JSONObject) parser.parse(responseString);
                    long curDur = (long) ((org.json.simple.JSONObject) json.get("data")).get("milliseconds");
                    duration += curDur;
//                    System.out.println(curDur + " ms");
                } catch (Exception ex) {
                    System.out.println("-- PROBLEM PARSING: " + file.getPath() + " --");
                }

            }
        }
        return duration;
    }

    /**
     * Executes a SPARQL query.
     *
     * @param queryStr: A String that holds the SPARQL query to be submitted on
     * the server.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @param format: The {link @QueryResultFormat} type of the query results.
     * @return The results of the submitted query w.r.t. the requested format.
     * @throws java.io.UnsupportedEncodingException
     */
    public Response executeSparqlQuery(String queryStr, String namespace, QueryResultFormat format) throws UnsupportedEncodingException {
        String mimetype = Utils.fetchQueryResultMimeType(format);
        return executeSparqlQuery(queryStr, namespace, mimetype);
    }

    public Response executeSparqlQuery(String queryStr, QueryResultFormat format) throws UnsupportedEncodingException {
        String mimetype = Utils.fetchQueryResultMimeType(format);
        return executeSparqlQuery(queryStr, namespace, mimetype);
    }

    /**
     * Executes a SPARQL query.
     *
     * @param queryStr: A String that holds the SPARQL query to be submitted on
     * the server.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @param mimetypeFormat: The mimetype format of the query results. The
     * accepted mimetypes can be found in:
     * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#SPARQL_Result_Sets">https://wiki.blazegraph.com/wiki/index.php/REST_API#SPARQL_Result_Sets</a>.
     * @return The results of the submitted query w.r.t. the requested format.
     * @throws UnsupportedEncodingException
     */
    public Response executeSparqlQuery(String queryStr, String namespace, String mimetypeFormat) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", URLEncoder.encode(queryStr, "UTF-8").replaceAll("\\+", "%20"))
                .queryParam("timeout", "" + this.timeout);
        Invocation.Builder invocationBuilder = webTarget.request(mimetypeFormat);
        Response response = invocationBuilder.get();
//        client.close();
        return response;
    }

    public Response executeSparqlQuery(String queryStr, String mimetypeFormat) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", URLEncoder.encode(queryStr, "UTF-8").replaceAll("\\+", "%20"))
                .queryParam("timeout", "" + this.timeout);;
        Invocation.Builder invocationBuilder = webTarget.request(mimetypeFormat);
        Response response = invocationBuilder.get();
//        client.close();
        return response;
    }

    public Response executeSparqlQueryEncoded(String queryStrEncoded, String namespace, String mimetypeFormat) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", queryStrEncoded)
                .queryParam("timeout", "" + this.timeout);
        Invocation.Builder invocationBuilder = webTarget.request(mimetypeFormat);
        Response response = invocationBuilder.get();
        return response;
    }

    public Response executeSparqlQueryEncoded(String queryStrEncoded, String mimetypeFormat) throws UnsupportedEncodingException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(serviceUrl + "/namespace/" + namespace + "/sparql")
                .queryParam("query", queryStrEncoded)
                .queryParam("timeout", "" + this.timeout);
        Invocation.Builder invocationBuilder = webTarget.request(mimetypeFormat);
        Response response = invocationBuilder.get();
        return response;
    }

    /**
     * Executes synchronously an update SPARQL query.
     *
     * @param queryStr: A String that holds the update query to be submitted on
     * the server.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @return The response of the update request
     * @throws java.io.UnsupportedEncodingException
     */
    public Response executeSparqlUpdateQuery(String queryStr, String namespace) throws UnsupportedEncodingException {
        String restURL = serviceUrl + "/namespace/" + namespace;
        // Taking into account nameSpace in the construction of the URL
        if (namespace != null) {
            restURL = serviceUrl + "/namespace/" + namespace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(restURL);
        String contentType = "application/sparql-update";
        Invocation.Builder invocationBuilder = webTarget.request(contentType);
        Response response = invocationBuilder.post(Entity.entity(queryStr, contentType));
        return response;
    }

    public Response executeSparqlUpdateQuery(String queryStr) throws UnsupportedEncodingException {
        String restURL;
        // Taking into account nameSpace in the construction of the URL
        if (namespace != null) {
            restURL = serviceUrl + "/namespace/" + namespace + "/sparql";
        } else {
            restURL = serviceUrl + "/sparql";
        }
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
     * @param queryStr: A String that holds the query to be submitted on the
     * server.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @throws java.io.UnsupportedEncodingException
     */
    public void executeAsyncSparqlUpdateQuery(String queryStr, String namespace) throws UnsupportedEncodingException {
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

    /**
     * Returns the number of the triples contained in the named graph given as
     * parameter. Moreover, the namespace repository should also be given as a
     * parameter.
     *
     * @param graph: The named graph whose triples are counted.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @return: The number of triples of the named graph.
     * @throws UnsupportedEncodingException
     */
    public long triplesNum(String graph, String namespace) throws UnsupportedEncodingException {
        String query = "select (count(*) as ?count) from <" + graph + "> where {?s ?p ?o}";
        Response response = executeSparqlQuery(query, namespace, QueryResultFormat.JSON);
        JSONObject json = new JSONObject(response.readEntity(String.class));
        JSONObject count = (JSONObject) json.getJSONObject("results").getJSONArray("bindings").get(0);
        return count.getJSONObject("count").getLong("value");
    }

    public long triplesNum(String graph) throws UnsupportedEncodingException {
        String query = "select (count(*) as ?count) from <" + graph + "> where {?s ?p ?o}";
        Response response = executeSparqlQuery(query, namespace, QueryResultFormat.JSON);
        JSONObject json = new JSONObject(response.readEntity(String.class));
        JSONObject count = (JSONObject) json.getJSONObject("results").getJSONArray("bindings").get(0);
        return count.getJSONObject("count").getLong("value");
    }

    /**
     * Returns the number of results for a given SPARQL query issued on a
     * specific namespace.
     *
     * @param query: The SPARQL query which results will be counted.
     * @param namespace: The namespace repository in which the SPARQL query will
     * be submitted.
     * @return: The number of query results.
     * @throws Exception
     */
    public long countSparqlResults(String query, String namespace) throws Exception {
        String queryTmp = query.toLowerCase();
        int end = queryTmp.indexOf("from");
        if (end == -1) {
            end = queryTmp.indexOf("where");
        }
        int start = queryTmp.indexOf(" ");
        StringBuilder sb = new StringBuilder();
        sb.append(query.substring(0, start)).append(" (count(*) as ?count) ").append(query.substring(end));
        JSONObject json = new JSONObject(executeSparqlQuery(sb.toString(), namespace, QueryResultFormat.JSON).readEntity(String.class));
        JSONObject count = (JSONObject) json.getJSONObject("results").getJSONArray("bindings").get(0);
        return count.getJSONObject("count").getLong("value");
    }
}
