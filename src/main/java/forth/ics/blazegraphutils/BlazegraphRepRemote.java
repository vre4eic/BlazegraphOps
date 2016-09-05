/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils;

import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import static forth.ics.blazegraphutils.BlazegraphRepLocal.loadProperties;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
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

    public BlazegraphRepRemote(String propFile, String serviceUrl) throws Exception {
        this.serviceUrl = serviceUrl;
        repository = new RemoteRepositoryManager(serviceUrl, false);
        properties = loadProperties(propFile);
    }

    public BlazegraphRepRemote(Properties propFile, String serviceUrl) throws Exception {
        this.serviceUrl = serviceUrl;
        repository = new RemoteRepositoryManager(serviceUrl, false);
        properties = propFile;
    }

    public void terminate() {
        try {
            repository.close();

        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /*
     * Status request.
     */
    public JettyResponseListener getStatus() throws Exception {
        final ConnectOptions opts = new ConnectOptions(this.serviceUrl + "/status");
        opts.method = "GET";
        return repository.doConnect(opts);
    }

    public void deleteNamespace(String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
//            System.out.println("Namespace: " + namespace + " was not found. ");
        } else {
//            System.out.println("Deleting namespace: " + namespace);
            this.repository.deleteRepository(namespace);
//            System.out.println("--- Done ---");
        }

    }

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

    public void importFile(String filename, RDFFormat format, String namespace) throws Exception {
//        System.out.print("Importing file: " + filename + " into namespace: " + namespace);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filename));
        loadDataFromResource(bis, format, namespace);
//        System.out.println("...DONE");
    }

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

    public void executeSPARULQuery(String sparul, String namespace) throws Exception {
        if (!namespaceExists(namespace)) {
            System.out.println("Namespace: " + namespace + " was not found. ");
        } else {
            repository.getRepositoryForNamespace(namespace)
                    .prepareUpdate(sparul).evaluate();
        }
    }

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

    public void clearGraphContents(String namespace, String graph) throws Exception {
        repository.getRepositoryForNamespace(namespace).prepareUpdate("CLEAR GRAPH <" + graph + ">").evaluate();
    }

    public void importDatasetTest(String filename, RDFFormat format, String namespace, int runs) throws Exception {
        long duration = 0;
        System.out.println("-- " + namespace + " --");
        for (int i = 0; i < runs; i++) {
            System.out.print("[run: " + i + "] ");
            deleteNamespace(namespace);
            createNamespace(namespace);
            long start = System.currentTimeMillis();
            if (new File(filename).isDirectory()) {
                importFolder(filename, format, namespace);
            } else {
                importFile(filename, format, namespace);
            }
            long curDur = System.currentTimeMillis() - start;
            System.out.println(curDur);
            duration += curDur;
        }
        System.out.println(namespace + "\t" + triplesNum(namespace, null) + "\t" + (duration / runs));
        System.out.println("----");
    }

    public static void main(String[] args) throws Exception {
        String serviceUrl = "http://83.212.97.61:9999/blazegraph";
//        serviceUrl = "http://139.91.183.48:9999/blazegraph";
        BlazegraphRepRemote blaze = new BlazegraphRepRemote("/config/quads.properties", serviceUrl);
        int runs = 5;
        //real data
//        blaze.importDatasetTest("C:/RdfData/cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, "cidoc-3_2_1", 1);
//        blaze.importDatasetTest("C:/RdfData/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "efo-2_48", runs);
//        blaze.importDatasetTest("C:/RdfData/EFO - 2.68.owl", RDFFormat.RDFXML, "efo-2_68", runs);
//        blaze.importDatasetTest("C:/RdfData/EFO - 2.691.owl", RDFFormat.RDFXML, "efo-2_691", runs);
//        blaze.importDatasetTest("C:/RdfData/Worms", RDFFormat.TURTLE, "worms", runs);
//        blaze.importDatasetTest("C:/RdfData/Fishbase", RDFFormat.TURTLE, "fishbase", runs);
//        blaze.importDatasetTest("C:/RdfData/Lifewatch", RDFFormat.NTRIPLES, "lifewatch", runs);

        //synthetic data
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/01. very small", RDFFormat.NTRIPLES, "lifewatch_very_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/02. small", RDFFormat.NTRIPLES, "lifewatch_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/03. med-small", RDFFormat.NTRIPLES, "lifewatch_medium_small", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/04. med-large", RDFFormat.NTRIPLES, "lifewatch_medium_large", runs);
//        blaze.importDatasetTest("C:/RdfData/LifeWatchSyntheticDatasets/05. large", RDFFormat.NTRIPLES, "lifewatch_large", runs);
        String namespace = "cidoc-3_2_1";
        String graph = "http://cidoc/3.2.1";

//        for (File file : new File("C:/RdfData/LifeWatchGreece_Queries").listFiles()) {
//            String query = readData(file.getAbsolutePath());
//            long start = System.currentTimeMillis();
////            blaze.executeSparqlQuery(query, namespace);
//            blaze.countSparqlResults(query, namespace);
//            System.out.println(file.getName() + "\t" + (System.currentTimeMillis() - start) + "\t" + blaze.countSparqlResults(query, namespace));
//        }
        System.out.println("triples: " + blaze.triplesNum(namespace, graph));
//        blaze.clearGraphContents(namespace, graph);
//        System.out.println("triples: " + blaze.triplesNum(namespace, graph));

        blaze.terminate();

    }

    public static String readData(String filename) {
        File f = new File(filename);
        String s = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = null;
            while ((line = br.readLine()) != null) {
                s += (line + "\n");
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage() + " occured .");
            return null;
        }
        return s;
    }
}
