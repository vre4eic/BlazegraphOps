/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils.tests;

import forth.ics.blazegraphutils.BlazegraphRepRemote;
import forth.ics.blazegraphutils.BlazegraphRepRestful;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
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
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
        System.out.println("-- " + graph + " --");
        Response response;
        long min = Long.MAX_VALUE, max = 0;
        for (int i = 0; i < runs; i++) {
            long curDur = 0;
            blaze.clearGraphContents(graph, namespace);
            if (new File(filename).isDirectory()) {
                curDur = blaze.importFolder(filename, format, namespace, graph);
            } else {
                JSONObject json = XML.toJSONObject(blaze.importFile(filename, format, namespace, graph).readEntity(String.class));
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
        System.out.println(graph + ": " + blaze.triplesNum(graph, namespace) + "\t\t\tDuration: " + duration / runs);
        System.out.println("----");
    }

    public static void main(String[] args) throws Exception {
        int runs = 5;
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.40:9999/blazegraph";
        service = "http://83.212.97.61:9999/blazegraph";

        String folder = null;
        folder = "C:/RdfData";
        if (args.length == 2) {
            if (args[0].equals("-path")) {
                folder = args[1];
            } else {
                folder = "C:/RdfData";
            }
        }
        String namespace = "quads_repo";
//        namespace = "test_ns";
//        blaze.deleteNamespace("worms");
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
//        blaze.createNamespace(propFile, namespace);
//        blaze

//        ImportDatasetTest(propFile, service, folder + "/cidoc_v3.2.1.rdfs", RDFFormat.NTRIPLES, "http://cidoc/3.2.1", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo/2.48", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/EFO - 2.68.owl", RDFFormat.RDFXML, "http://efo/2.68", namesapce, runs);
//        ImportDatasetTest(propFile, service, folder + "/EFO - 2.691.owl", RDFFormat.RDFXML, "http://efo/2.691", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/Lifewatch", RDFFormat.NTRIPLES, "http://lifewatchgreece.com", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/Worms", RDFFormat.TURTLE, "http://worms", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/Fishbase", RDFFormat.TURTLE, "http://fishbase", namespace, runs);
        //synthetic data
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/01. very small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/vsmall", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/02. small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/small", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/03. med-small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medsmall", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/04. med-large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medlarge", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/05. large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/06. very large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/vlarge", namespace, runs);
//        ImportDatasetTest(propFile, service, folder + "/LifeWatchSyntheticDatasets/07. huge", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/huge", namespace, runs);
//        long start = System.currentTimeMillis();
        String graph = "http://lifewatchgreece.com";
        graph = "http://lifewatchgreece.com/vlarge";
//        String namespaceRepo = "lifewatch";
//        namespaceRepo = "lifewatch_large";
        queryTest(folder, graph, runs, blaze, namespace);
    }

    public static void queryTest(String folder, String graph, int runs, BlazegraphRepRestful blaze, String namespaceRepo) throws Exception {
        for (File file : new File(folder).listFiles()) {
            System.out.println("-- " + file.getName() + " --");
            if (file.isDirectory()) {
                continue;
            }
            long duration = 0;
            long min = Long.MAX_VALUE, max = 0;
            String query = BlazegraphRepRemote.readData(file.getAbsolutePath());
            query = query.replace("[namegraph]", "<" + graph + ">");
            int i;
//            System.out.println(query);
            for (i = 0; i < runs; i++) {
                long start = System.currentTimeMillis();
//                TupleQueryResult result = graphDB.executeSparqlQuery(query);
//                while (result.hasNext()) {
//                    result.next();
//                }
//                result.close();
//                blaze.executeSparqlQuery(query, namespaceRepo, QueryResultFormat.CSV);
                blaze.countSparqlResults(query, namespaceRepo);
                long curDur = (System.currentTimeMillis() - start);

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
            i -= 2;
//            System.out.println(file.getName() + "\t" + duration / i);
            System.out.println(file.getName() + "\t" + duration / i + "\t" + blaze.countSparqlResults(query, namespaceRepo));
//            break;
        }

//        Response response = blaze.clearGraphContent("http://cidoc/3.2.1", "cidoc-3_2_1");
//        Response response = blaze.clearGraphContent("http://cidoc/3.2.1", "cidoc-3_2_1");
//        System.out.println(response.readEntity(String.class));
//        blaze.deleteNamespace("efo-2_48");
//        new BlazegraphRepRestful(service).deleteNamespace("efo-2_48");
//        blaze.createNamespace("/config/quads.properties", "test-namespace");
//        blaze.executeSparqlUpdateQuery("insert data into <http://test> {<http://a> rdf:type rdfs:Class. }", "test-namespace");
    }
}
