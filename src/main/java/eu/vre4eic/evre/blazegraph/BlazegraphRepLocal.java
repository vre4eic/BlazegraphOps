/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
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
     * @param propFile: The Blazegraph properties file's full path which will be
     * considered.
     * @throws IOException: If the properties file is not found.
     * @throws org.openrdf.repository.RepositoryException: If any error occurs
     * upon the initialization of the repository.
     */
    public BlazegraphRepLocal(String propFile) throws IOException, RepositoryException {
        // load journal properties from resources
        Properties props = loadProperties(propFile);
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
        for (File file : new File(folder).listFiles()) {
//            System.out.println("Importing file: " + file.getName() + " into graph: " + graphDest);
            InputStreamReader in;
            try {
                in = new InputStreamReader(new FileInputStream(file), "UTF8");
                con.add(in, "", format, new URIImpl(graphDest));
            } catch (Exception ex) {
                System.out.println("Exception: " + ex.getMessage());
            }
//            System.out.println("--- Done ---");
        }
        con.commit();
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

//        blaze.importFile("C:\\RdfData\\_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo-2.48");
        System.out.println(blaze.triplesNum("http://efo-2.48"));

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
