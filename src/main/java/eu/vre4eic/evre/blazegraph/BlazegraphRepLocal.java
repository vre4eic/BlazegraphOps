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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.openrdf.OpenRDFException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

/**
 *
 * @author rous
 */
public class BlazegraphRepLocal {

    private Repository repository;
    private RepositoryConnection con;

    /**
     * Creates a local Blazegraph repository connection.
     *
     * @param embedPropsFile: The Blazegraph properties file's full path which
     * will be considered.
     * @throws IOException: If the properties file is not found.
     * @throws org.openrdf.repository.RepositoryException: If any error occurs
     * upon the initialization of the repository.
     */
    public BlazegraphRepLocal(String embedPropsFile) throws IOException, RepositoryException {
        // load journal properties from resources
        Properties props = loadProperties(embedPropsFile);
        // instantiate a sail
        BigdataSail sail = new BigdataSail(props);
        repository = new BigdataSailRepository(sail); // create a Sesame repository
        repository.initialize();
        con = repository.getConnection();
    }

    public BlazegraphRepLocal(Properties props) throws IOException, RepositoryException {
        // instantiate a sail
        BigdataSail sail = new BigdataSail(props);
        repository = new BigdataSailRepository(sail); // create a Sesame repository
        repository.initialize();
        con = repository.getConnection();
    }

    private static Properties loadProperties(String resource) throws IOException {
        Properties p = new Properties();
        InputStream is = BlazegraphRepLocal.class
                .getResourceAsStream(resource);
        p.load(new InputStreamReader(new BufferedInputStream(is)));
        return p;
    }

    /**
     * Imports a file with RDF contents within Blazegraph and the named graph
     * given as parameter. The file must belong in the resources folder of the
     * project.
     *
     * @param resource: The resources file which contains the RDF data to be
     * inserted.
     * @param format: The format of the give data e.g., RDF/XML, N3, N-Triples
     * etc.
     * @param graphDest: The named graph destination.
     * @throws IOException: If the data file cannot be found in the resources
     * folder.
     */
    public void loadDataFromResources(String resource, RDFFormat format, String graphDest) throws IOException {
        String JDK_ENTITY_EXPANSION_LIMIT = "jdk.xml.entityExpansionLimit";
        System.setProperty(JDK_ENTITY_EXPANSION_LIMIT, "0");
        System.out.println("Importing file into graph: " + graphDest);
        try {
            con.begin();
            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                InputStream is = classLoader.getResourceAsStream(resource);
                if (is == null) {
                    throw new IOException("Could not locate resource: " + resource);
                }
                InputStreamReader in = new InputStreamReader(is, "UTF8");
                con.add(in, "", format, new URIImpl(graphDest));
                con.commit();
            } catch (OpenRDFException ex) {
                con.rollback();
            }
        } catch (RepositoryException ex) {
            java.util.logging.Logger.getLogger(BlazegraphRepLocal.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("--- Done ---");
        System.clearProperty(JDK_ENTITY_EXPANSION_LIMIT);
    }

    /**
     * Imports a file with RDF contents within Blazegraph and the named graph
     * given as parameter.
     *
     * @param file: The filename which contains the RDF data to be inserted.
     * @param format: The format of the give data e.g., RDF/XML, N3, N-Triples
     * etc.
     * @param graphDest: The named graph destination.
     * @throws java.io.IOException: If the data file cannot be found.
     */
    public void importFile(String file, RDFFormat format, String graphDest) throws IOException {
        String JDK_ENTITY_EXPANSION_LIMIT = "jdk.xml.entityExpansionLimit";
        System.setProperty(JDK_ENTITY_EXPANSION_LIMIT, "0");
        System.out.println("Importing file into graph: " + graphDest);
        try {
            con.begin();
            try {
                InputStreamReader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), "UTF-8");
                try {
                    con.add(reader, "", format, new URIImpl(graphDest));
                } finally {
                    reader.close();
                }
                con.commit();
            } catch (OpenRDFException ex) {
                con.rollback();
            }
        } catch (RepositoryException ex) {
            java.util.logging.Logger.getLogger(BlazegraphRepLocal.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("--- Done ---");
        System.clearProperty(JDK_ENTITY_EXPANSION_LIMIT);
    }

    /**
     * Imports a set of files with RDF data located in a folder into Blazegraph
     * and a specific named graph. The files must be in the same format.
     *
     * @param folder: The folder path which contains the RDF data files.
     * @param format: The format of the files to be inserted e.g., RDF/XML, N3,
     * N-Triples etc.
     * @param graphDest: The named graph destination.
     * @throws org.openrdf.repository.RepositoryException: In case something
     * goes wrong during the data import.
     */
    public void importFolder(String folder, RDFFormat format, String graphDest) throws RepositoryException {
        System.out.println("Importing Folder: " + folder + " into graph: " + graphDest);
        for (File file : new File(folder).listFiles()) {
//            System.out.println("Importing file: " + file.getName() + " into graph: " + graphDest);
            InputStreamReader in;
            try {
                in = new InputStreamReader(new FileInputStream(file), "UTF8");
                con.add(in, "", format, new URIImpl(graphDest));
            } catch (Exception ex) {
                System.out.println("Exception: " + ex.getMessage());
            }
        }
        con.commit();
        System.out.println("--- Done ---");

    }

    /**
     * Clears the contents of the named graph given as parameter.
     *
     * @param graph: The named graph to be cleared.
     * @throws RepositoryException: In case something goes wrong during the data
     * deletion from the repository.
     */
    public void clearGraphContents(String graph) throws RepositoryException {
        System.out.println("Deleting contents of: " + graph);
        con.clear(new URIImpl(graph));
        con.commit();
    }

    /**
     * Executes a SPARQL select query given as parameter.
     *
     * @param sparql The SPARQL query to be executed.
     * @return A {@link TupleQueryResult} object which can be iterated to get
     * the query results.
     * @throws RepositoryException: In case something goes wrong during the data
     * access from the repository.
     * @throws MalformedQueryException: In case the query is not a syntactically
     * correct SPARQL.
     * @throws QueryEvaluationException: In case something goes wrong during the
     * query evaluation.
     */
    public TupleQueryResult executeSparqlQuery(String sparql) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
        TupleQueryResult result = tupleQuery.evaluate();
        return result;
    }

    public void executeSparqlUpdate(String sparul) throws RepositoryException, MalformedQueryException, QueryEvaluationException, UpdateExecutionException {
        Update updateQueryQuery = (Update) con.prepareUpdate(QueryLanguage.SPARQL, sparul);
        updateQueryQuery.execute();
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
        con.export(writer, new URIImpl(graphSource));
    }

    /**
     * Returns the number of the triples contained in the named graph given as
     * parameter.
     *
     * @param graph The named graph whose triples are counted.
     * @return The number of triples.
     */
    public long triplesNum(String graph) {
        long result = 0;
        try {
            TupleQueryResult res = executeSparqlQuery("select (count(*) as ?num) from <" + graph + "> where { ?s ?p ?o . }");
            while (res.hasNext()) {
                BindingSet set = res.next();
                result = Long.parseLong(set.getValue("num").stringValue());
            }
        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException ex) {
            Logger.getLogger(BlazegraphRepLocal.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            return result;
        }
    }

    /**
     * Inserts a (URI) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The object URI triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addTriple(String s, String p, String o, String graph) {
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        URI obj = repository.getValueFactory().createURI(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /**
     * Inserts a (Literal) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The string literal object of the triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addLitTriple(String s, String p, String o, String graph) {
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        Literal obj = repository.getValueFactory().createLiteral(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /**
     * Inserts a (Literal) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The double literal object of the triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addLitTriple(String s, String p, double o, String graph) {
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        Literal obj = repository.getValueFactory().createLiteral(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /**
     * Terminates the RepositoryConnection connection.
     */
    public void terminate() {
        try {
            con.close();
            repository.shutDown();
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
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
    public long countSparqlResults(String query) throws Exception {
        long result = 0;
        String queryTmp = query.toLowerCase();
        int end = queryTmp.indexOf("from");
        if (end == -1) {
            end = queryTmp.indexOf("where");
        }
        int start = queryTmp.indexOf(" ");
        StringBuilder sb = new StringBuilder();
        sb.append(query.substring(0, start)).append(" (count(*) as ?triples) ").append(query.substring(end));
        TupleQueryResult res = executeSparqlQuery(sb.toString());
        while (res.hasNext()) {
            result = Long.parseLong(res.next().getValue("triples").stringValue());
        }
        res.close();
        return result;
    }

    /**
     *
     * @param filename
     * @param format
     * @param graph
     * @throws Exception
     */
    public void importDatasetTest(String filename, RDFFormat format, String graph) throws Exception {
        clearGraphContents(graph);
        long start = System.currentTimeMillis();
        if (new File(filename).isDirectory()) {
            importFolder(filename, format, graph);
        } else {
            importFile(filename, format, graph);
        }
        System.out.println(graph + "\t" + triplesNum(graph) + "\t" + (System.currentTimeMillis() - start));

    }

    public static void main(String[] args) throws Exception {
        BlazegraphRepLocal blaze = new BlazegraphRepLocal("/config/quads.properties");

        String graph = "http://efo-2.48";
//        blaze.clearGraphContents(graph);
        System.out.println(blaze.triplesNum(graph));
//        blaze.importFile("C:\\RdfData\\_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, graph);
//        System.out.println(blaze.triplesNum(graph));
//        System.out.println(blaze.triplesNum("http://efo-2.48"));

//        InputStream is = classLoader.getResourceAsStream("EFO - 2.691.owl");
//        blaze.importFile(is, RDFFormat.RDFXML, "http://efo/2.691");
//        System.out.println("Duration: " + (System.currentTimeMillis() - start));
//        blaze.importDatasetTest("input/cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, "http://cidoc/3.2.1");
//        blaze.importDatasetTest("input/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo/2.48");
//        blaze.importDatasetTest("input/EFO - 2.68.owl", RDFFormat.RDFXML, "http://efo/2.68");
//        blaze.importDatasetTest("input/EFO - 2.691.owl", RDFFormat.RDFXML, "http://efo/2.691");
//        blaze.importDatasetTest("input/LifeWatchDatabase.ttl", RDFFormat.TURTLE, "http://lifewatch");
//        blaze.importDatasetTest("input/Fishbase", RDFFormat.TURTLE, "http://fishbase");
//        blaze.importDatasetTest("input/Worms", RDFFormat.TURTLE, "http://worms");
//        blaze.importDatasetTest("input/lifewatch5a.nt_fixed", RDFFormat.NTRIPLES, "http://lifewatch");       
        blaze.terminate();
    }
}
