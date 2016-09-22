/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils;

import java.io.File;
import javax.ws.rs.core.Response;
import org.json.JSONObject;
import org.json.XML;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author rousakis
 */
public class TestBlazegraphRest {

    public static void ImportDatasetTest(String properties, String service, String filename, RDFFormat format, String graph, String namespace, int runs) throws Exception {
        long duration = 0;
        BlazegraphRepRestful blazeRest = new BlazegraphRepRestful(service);
        System.out.println("-- " + graph + " --");
        Response response;
        long min = Long.MAX_VALUE, max = 0;
        for (int i = 0; i < runs; i++) {
            long curDur = 0;
            blazeRest.clearGraphContents(graph, namespace);
            if (new File(filename).isDirectory()) {
                curDur = blazeRest.importFolder(filename, format, namespace, graph);
            } else {
                response = blazeRest.importFile(filename, format, namespace, graph);
                JSONObject json = XML.toJSONObject(response.readEntity(String.class));
                curDur = ((JSONObject) json.get("data")).getLong("milliseconds");
            }
            if (min > curDur) {
                min = curDur;
            }
            if (max < curDur) {
                max = curDur;
            }
            System.out.println(curDur);
            duration += curDur;
        }
        duration = duration - min - max;
        runs -= 2;
        System.out.println(graph + ": " + blazeRest.triplesNum(graph, namespace) + "\t\t\tDuration: " + duration / runs);
        System.out.println("----");
    }

    public static void main(String[] args) throws Exception {
        int runs = 5;
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.40:9999/blazegraph";
//        service = "http://83.212.97.61:9999/blazegraph";
        String namespaceRepo = "dbpedia";
        BlazegraphRepRemote blaze = new BlazegraphRepRemote(propFile, service);
//        blaze.deleteNamespace(namespaceRepo);
//        blaze.createNamespace(namespaceRepo);
//       
        blaze.terminate();

//        ImportDatasetTest(propFile, service, "C:/RdfData/cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, "http://cidoc/3.2.1", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo/2.48", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/EFO - 2.68.owl", RDFFormat.RDFXML, "http://efo/2.68", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/EFO - 2.691.owl", RDFFormat.RDFXML, "http://efo/2.691", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/Lifewatch", RDFFormat.NTRIPLES, "http://lifewatchgreece.com", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/Worms", RDFFormat.TURTLE, "http://worms", namespaceRepo, runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/Fishbase", RDFFormat.TURTLE, "http://fishbase", namespaceRepo, runs);
        //synthetic data
//        ImportDatasetTest(propFile, service, "C:/RdfData/LifeWatchSyntheticDatasets/01. very small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/vsmall", "lifewatch_very_small", runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/LifeWatchSyntheticDatasets/02. small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/small", "lifewatch_small", runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/LifeWatchSyntheticDatasets/03. med-small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medsmall", "lifewatch_medium_small", runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/LifeWatchSyntheticDatasets/04. med-large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medlarge", "lifewatch_medium_large", runs);
//        ImportDatasetTest(propFile, service, "C:/RdfData/LifeWatchSyntheticDatasets/05. large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", "lifewatch_large", runs);
//        long start = System.currentTimeMillis();
//        JSONObject xmlJSONObj = XML.toJSONObject(response.readEntity(String.class));
//        System.out.println(xmlJSONObj);
//        System.out.println(System.currentTimeMillis() - start);
//        for (File file : new File("C:\\Dropbox\\Shared Netbeans Projects\\Forth Projects\\VirtuosoOps\\input\\LifeWatchGreece_Queries").listFiles()) {
//            String query = BlazegraphRepRemote.readData(file.getAbsolutePath());
//            long start = System.currentTimeMillis();
////            blaze.executeSparqlQuery(query, namespace, QueryResultFormat.JSON);
//            blaze.countSparqlResults(query, namespace);
//            System.out.println(file.getName() + "\t" + (System.currentTimeMillis() - start) + "\t" + blaze.countSparqlResults(query, namespace));
//        }
//        Response response = blaze.clearGraphContent("http://cidoc/3.2.1", "cidoc-3_2_1");
//        Response response = blaze.clearGraphContent("http://cidoc/3.2.1", "cidoc-3_2_1");
//        System.out.println(response.readEntity(String.class));
//        blaze.deleteNamespace("efo-2_48");
//        new BlazegraphRepRestful(service).deleteNamespace("efo-2_48");
//        blaze.createNamespace("/config/quads.properties", "test-namespace");
//        blaze.executeSparqlUpdateQuery("insert data into <http://test> {<http://a> rdf:type rdfs:Class. }", "test-namespace");
    }
}
