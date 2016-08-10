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
        System.out.println("-- " + namespace + " --");
        Response response;
        for (int i = 0; i < runs; i++) {
            BlazegraphRepRemote blaze = new BlazegraphRepRemote(properties, service);
            blaze.deleteNamespace(namespace);
            blaze.createNamespace(namespace);
            blaze.terminate();
            long curDur = 0;
            if (new File(filename).isDirectory()) {
                curDur = blazeRest.importFolder(filename, format, namespace, graph);
            } else {
                response = blazeRest.importFile(filename, format, namespace, graph);
                JSONObject json = XML.toJSONObject(response.readEntity(String.class));
                curDur = ((JSONObject) json.get("data")).getLong("milliseconds");
            }
            System.out.println(curDur);
            duration += curDur;
        }
        System.out.println(graph + ": " + blazeRest.triplesNum(graph, namespace) + "\t\t\tDuration: " + duration / runs);
        System.out.println("----");
    }

    public static void main(String[] args) throws Exception {
        //BlazegraphRepRestful blaze = new BlazegraphRepRestful("/config/quads.properties", "http://139.91.183.88:9999/blazegraph/sparql");

        int runs = 10;
        String propFile = "/config/quads.properties";
        String service = "http://83.212.97.61:9999/blazegraph";

//        ImportDatasetTest(propFile, service, "C:/RdfData/cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, "http://cidoc/3.2.1", "cidoc-3_2_1", 1);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/_diachron_efo-2.48.nt", RDFFormat.NTRIPLES, "http://efo/2.48", "efo-2_48", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/EFO - 2.68.owl", RDFFormat.RDFXML, "http://efo/2.68", "efo-2_68", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/EFO - 2.691.owl", RDFFormat.RDFXML, "http://efo/2.691", "efo-2_691", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/Worms", RDFFormat.TURTLE, "http://worms", "worms", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/Fishbase", RDFFormat.TURTLE, "http://fishbase", "fishbase", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/Lifewatch", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", "lifewatch_large", 1);
        //synthetic data
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/LifeWatchSyntheticDatasets/01. very small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/vsmall", "lifewatch_very_small", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/LifeWatchSyntheticDatasets/02. small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/small", "lifewatch_small", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/LifeWatchSyntheticDatasets/03. med-small", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medsmall", "lifewatch_medium_small", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/LifeWatchSyntheticDatasets/04. med-large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/medlarge", "lifewatch_medium_large", runs);
//        blaze.importDatasetTest("/config/quads.properties","C:/RdfData/LifeWatchSyntheticDatasets/05. large", RDFFormat.NTRIPLES, "http://lifewatchgreece.com/large", "lifewatch_large", runs);
//        long start = System.currentTimeMillis();
//        JSONObject xmlJSONObj = XML.toJSONObject(response.readEntity(String.class));
//        System.out.println(xmlJSONObj);
//        System.out.println(System.currentTimeMillis() - start);
        String namespace = "lifewatch_large";
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
        new BlazegraphRepRestful(service).createNamespace("/config/quads.properties", "efo-2_48");

    }
}
