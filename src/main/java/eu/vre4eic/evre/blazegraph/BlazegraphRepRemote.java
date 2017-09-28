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

import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Properties;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

/**
 *
 * @author rousakis
 */
public class BlazegraphRepRemote {

    private RemoteRepositoryManager repository;
    private String serviceUrl;
    private Properties properties;

    /**
     *
     * @param propFile
     * @param serviceUrl
     * @throws Exception
     */
    public BlazegraphRepRemote(String propFile, String serviceUrl) throws Exception {
        this.serviceUrl = serviceUrl;
        repository = new RemoteRepositoryManager(serviceUrl, false);
        properties = loadProperties(propFile);
    }

    /**
     *
     * @param propFile
     * @param serviceUrl
     * @throws Exception
     */
    public BlazegraphRepRemote(Properties propFile, String serviceUrl) throws Exception {
        this.serviceUrl = serviceUrl;
        repository = new RemoteRepositoryManager(serviceUrl, false);
        properties = propFile;
    }

    /**
     * Closes a repository connection.
     */
    public void terminate() {
        try {
            repository.close();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    private static Properties loadProperties(String resource) throws IOException {
        Properties p = new Properties();
        InputStream is = BlazegraphRepLocal.class
                .getResourceAsStream(resource);
        p.load(new InputStreamReader(new BufferedInputStream(is)));
        return p;
    }

    /**
     *
     * @param namespace
     * @throws Exception
     */
    public void deleteNamespace(String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
//            System.out.println("Namespace: " + namespace + " was not found. ");
        } else {
//            System.out.println("Deleting namespace: " + namespace);
            this.repository.deleteRepository(namespace);
//            System.out.println("--- Done ---");
        }

    }

    /**
     *
     * @param namespace
     * @throws Exception
     */
    public void createNamespace(String namespace) throws Exception {
        repository.createRepository(namespace, properties);
    }

    private boolean namespaceExists(String namespace) throws Exception {
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
        }
        return false;
    }

    /**
     *
     * @param filename
     * @param format
     * @param namespace
     * @throws Exception
     */
    public void importFile(String filename, RDFFormat format, String namespace) throws Exception {
//        System.out.print("Importing file: " + filename + " into namespace: " + namespace);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filename));
        loadDataFromResource(bis, format, namespace);
//        System.out.println("...DONE");
    }

    /**
     *
     * @param folder
     * @param format
     * @param namespace
     * @throws Exception
     */
    public void importFolder(String folder, RDFFormat format, String namespace) throws Exception {
        for (File file : new File(folder).listFiles()) {
            if (!file.isDirectory()) {
//                System.out.print("Importing file: " + file.getName() + " into namespace: " + namespace);
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                repository.getRepositoryForNamespace(namespace).add(new RemoteRepository.AddOp(bis, format));
                bis.close();
//                System.out.println("...DONE");
            }
        }
    }

    /**
     *
     * @param is
     * @param format
     * @param namespace
     * @throws Exception
     */
    public void loadDataFromResource(InputStream is, RDFFormat format, String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
//            System.out.println(String.format("Create namespace %s...", namespace));
            createNamespace(namespace);
//            System.out.println(String.format("Create namespace %s done", namespace));
        } else {
//            System.out.println(String.format("Namespace %s already exists", namespace));
        }
        try {
//            System.out.println("Importing file into repository");
            repository.getRepositoryForNamespace(namespace).add(new RemoteRepository.AddOp(is, format));
//            System.out.println("--- Done ---");
        } finally {
            is.close();
        }
    }

    /**
     * Exports the contents of a named graph into a file in various formats.
     *
     * @param filename The filename in which the export data will be stored. The
     * filename can be either in the Virtuoso-host machine of not.
     * @param format The format of the exported data e.g., RDF/XML, N3,
     * N-Triples etc.
     * @param graphSource The named graph whose data will be exported.
     * @throws Exception
     */
    public void exportToFile(String filename, RDFFormat format, String graphSource) throws Exception {
        System.out.println("Exporting graph: " + graphSource.toString());
        RDFWriter writer = Rio.createWriter(format, new OutputStreamWriter(new FileOutputStream(new File(filename))));
    }

    public TupleQueryResult executeSPARQLQuery(String sparql, String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
            System.out.println("Namespace: " + namespace + " was not found. ");
            return null;
        } else {
            TupleQueryResult result = repository.getRepositoryForNamespace(namespace)
                    .prepareTupleQuery(sparql).evaluate();
            return result;
        }
    }

    /**
     *
     * @param sparul
     * @param namespace
     * @throws Exception
     */
    public void executeSPARULQuery(String sparul, String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
            System.out.println("Namespace: " + namespace + " was not found. ");
        } else {
            repository.getRepositoryForNamespace(namespace)
                    .prepareUpdate(sparul).evaluate();
        }
    }

    /**
     *
     * @param namespace
     * @param graph
     * @return
     * @throws Exception
     */
    public long triplesNum(String namespace, String graph) throws Exception {
        long result = 0;
        String graphClause = "";
        if (graph != null) {
            graphClause = "from <" + graph + ">";
        }
        String query = "select (count(*) as ?num) " + graphClause + " where { ?s ?p ?o . }";

        TupleQueryResult res = executeSPARQLQuery(query, namespace);
        while (res.hasNext()) {
            BindingSet set = res.next();
            result = Long.parseLong(set.getValue("num").stringValue());
        }
        return result;
    }

    /**
     *
     * @param query
     * @param namespace
     * @return
     * @throws Exception
     */
    public long countSparqlResults(String query, String namespace) throws Exception {
        long result = 0;
        String queryTmp = query.toLowerCase();
        int end = queryTmp.indexOf("from");
        if (end == -1) {
            end = queryTmp.indexOf("where");
        }
        int start = queryTmp.indexOf(" ");
        StringBuilder sb = new StringBuilder();
        sb.append(query.substring(0, start)).append(" (count(*) as ?triples) ").append(query.substring(end));
        TupleQueryResult res = executeSPARQLQuery(sb.toString(), namespace);
        while (res.hasNext()) {
            result = Long.parseLong(res.next().getValue("triples").stringValue());
        }
        res.close();
        return result;
    }

    /**
     *
     * @param namespace
     * @param graph
     * @throws Exception
     */
    public void clearGraphContents(String namespace, String graph) throws Exception {
        repository.getRepositoryForNamespace(namespace).prepareUpdate("CLEAR GRAPH <" + graph + ">").evaluate();
    }

    public static void main(String[] args) throws Exception {
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.46:9999/blazegraph"; //seistro
        service = "http://139.91.183.70:9999/blazegraph"; //seistro
        BlazegraphRepRemote remote = new BlazegraphRepRemote(propFile, service);
        String namespace = "ekt-data";

        remote.deleteNamespace(namespace);
//        
//        String query = "Select *  \n"
//                + "where\n"
//                + "{ \n"
//                + "{\n"
//                + "<http://www.oeaw.ac.at/COIN/626> ?p ?o .\n"
//                + "<http://www.oeaw.ac.at/COIN/626>  rdf:type ?stype .\n"
//                + "OPTIONAL {<http://www.oeaw.ac.at/COIN/626>  \n"
//                + "<http://www.w3.org/2000/01/rdf-schema#label>  ?slabel }.\n"
//                + "OPTIONAL {?p <http://www.w3.org/2000/01/rdf-schema#label> ?plabel }.\n"
//                + "OPTIONAL {?o <http://www.w3.org/2000/01/rdf-schema#label>  ?olabel }.\n"
//                + "OPTIONAL {?o rdf:type ?otype} .\n"
//                + "} \n"
//                + "  UNION\n"
//                + "{ \n"
//                + "<http://www.oeaw.ac.at/COIN/626> ?p ?o \n"
//                + ".\n"
//                + "<http://www.oeaw.ac.at/COIN/626>  rdf:type ?stype .\n"
//                + "OPTIONAL {<http://www.oeaw.ac.at/COIN/626>  \n"
//                + "<http://www.w3.org/2000/01/rdf-schema#label>  ?slabel }.\n"
//                + " OPTIONAL{?p <http://www.w3.org/2000/01/rdf-schema#label>  ?plabel }.\n"
//                + "\n"
//                + "  \n"
//                + "FILTER(isLiteral(?o))\n"
//                + "} \n"
//                + "}";

//        TupleQueryResult result = remote.executeSPARQLQuery(query, namespace);
//        List<String> names = result.getBindingNames();
//        while (result.hasNext()) {
//            BindingSet set = result.next();
//            for (String name : names) {
//                System.out.println(name + " : " + set.getValue(name).stringValue());
//            }
//        }
        remote.terminate();
    }

}
