/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils.tests;

import forth.ics.blazegraphutils.BlazegraphRepRestful;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author rousakis
 */
public class DBpediaTest {

    public static void main(String[] args) throws Exception {

        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.40:9999/blazegraph";
//        service = "http://83.212.97.61:9999/blazegraph";
        String namespaceRepo = "dbpedia";
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
//        BlazegraphRepRemote blazeRem = new BlazegraphRepRemote(propFile, service);
//        blaze.createNamespace(propFile, namespaceRepo);

//        System.out.println(blazeRem.triplesNum(namespaceRepo, "http://dbpedia3.8"));

//        String folder = "/home/rousakis/Datasets/ChangeDetectionDatasets/dbpedia/v3.8";
        String graph = "http://dbpedia3.8";
//        blaze.importFile("C:\\Users\\rousakis\\Desktop\\dbpedia_3.8.owl", RDFFormat.RDFXML, namespaceRepo, graph);
//        blazeRem.terminate();
        System.out.println(blaze.triplesNum(graph, namespaceRepo));
    }
}
