/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils;

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

    public BlazegraphRepLocal(String propFile) throws IOException, RepositoryException {
        // load journal properties from resources
        Properties props = loadProperties(propFile);
        // instantiate a sail
        BigdataSail sail = new BigdataSail(props);
        repository = new BigdataSailRepository(sail); // create a Sesame repository
        repository.initialize();
        con = repository.getConnection();
    }

    public static Properties loadProperties(String resource) throws IOException {
        Properties p = new Properties();
        InputStream is = BlazegraphRepLocal.class
                .getResourceAsStream(resource);
        p.load(new InputStreamReader(new BufferedInputStream(is)));
        return p;
    }

    /**
     * Imports a file with RDF contents within Virtuoso and the named graph
     * given as parameter.
     *
     * @param filename The filename which contains the data to be imported.
     * @param format The format of the give data e.g., RDF/XML, N3, N-Triples
     * etc.
     * @param graphDest The named graph destination.
     * @throws Exception
     */
    public void loadDataFromResources(String resource, RDFFormat format, String graphDest) throws IOException, OpenRDFException {
        String JDK_ENTITY_EXPANSION_LIMIT = "jdk.xml.entityExpansionLimit";
        System.setProperty(JDK_ENTITY_EXPANSION_LIMIT, "0");
        System.out.println("Importing file into graph: " + graphDest);
        try {
            con.begin();
            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                InputStream is = classLoader.getResourceAsStream(resource);
//                InputStream is = BlazegraphRepLocal.class.getResourceAsStream(resource);
                if (is == null) {
                    throw new IOException("Could not locate resource: " + resource);
                }
//                Reader reader = new InputStreamReader(new BufferedInputStream(is));
//                try {
//                    con.add(reader, "", RDFFormat.N3, graphDest);
                InputStreamReader in = new InputStreamReader(is, "UTF8");
                con.add(in, "", format, new URIImpl(graphDest));
//                } finally {
////                    reader.close();
//                }
                con.commit();
            } catch (OpenRDFException ex) {
                con.rollback();
                throw ex;
            }
        } catch (RepositoryException ex) {
            java.util.logging.Logger.getLogger(BlazegraphRepLocal.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("--- Done ---");
        System.clearProperty(JDK_ENTITY_EXPANSION_LIMIT);
    }

    public void importFile(String file, RDFFormat format, String graphDest) throws RepositoryException, IOException, OpenRDFException {
        String JDK_ENTITY_EXPANSION_LIMIT = "jdk.xml.entityExpansionLimit";
        System.setProperty(JDK_ENTITY_EXPANSION_LIMIT, "0");
//        System.out.println("Importing file into graph: " + graphDest);
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
            throw ex;
        }
//        System.out.println("--- Done ---");
        System.clearProperty(JDK_ENTITY_EXPANSION_LIMIT);
    }

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
     * Clears the named graph given as parameter.
     *
     * @param graph The named graph to be cleared.
     * @throws Exception
     */
    public void clearGraphContents(String graph) throws Exception {
        System.out.println("Deleting contents of: " + graph);
        con.clear(new URIImpl(graph));
    }

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

    public long triplesNum(String graph) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        long result = 0;
        TupleQueryResult res = executeSparqlQuery("select (count(*) as ?num) from <" + graph + "> where { ?s ?p ?o . }");
        while (res.hasNext()) {
            BindingSet set = res.next();
            result = Long.parseLong(set.getValue("num").stringValue());
        }
        return result;
    }

    public void terminate() {
        try {
            con.close();
            repository.shutDown();
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

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

//        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        InputStream is = classLoader.getResourceAsStream("LifeWatchDatabase.ttl");
//        blaze.importFile(is, RDFFormat.NTRIPLES, "http://lifewatch");
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
        blaze.importDatasetTest("input/lifewatch5a.nt_fixed", RDFFormat.NTRIPLES, "http://lifewatch");

        blaze.terminate();
    }
}
