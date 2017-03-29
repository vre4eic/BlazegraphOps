/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.vre4eic.evre.blazegraph;

import eu.vre4eic.evre.blazegraph.BlazegraphRepRestful;
import eu.vre4eic.evre.blazegraph.QueryResultFormat;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import javax.ws.rs.core.Response;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author rousakis
 */
public class RousTest {

    public static void main(String[] args) throws UnsupportedEncodingException, IOException, Exception {
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.46:9999/blazegraph"; //seistro
        service = "http://139.91.183.70:9999/blazegraph"; //seistro2
//        service = "http://139.91.183.40:9999/blazegraph"; //stalone
//        service = "http://83.212.97.61:9999/blazegraph";
        String namespace = "localtest";
        namespace = "ekt_data";
//        namespace = "test";
        namespace = "ekt-demo";
        String graph = "http://cidoc";
        graph = "http://ekt-data";
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
        String query = "SELECT ?s ?o ?p from <http://ekt-data> WHERE {{ ?s ?p ?o . ?s rdfs:label ?o. ?o bds:search 'Quadrelli' . }}";
        System.out.println(blaze.executeSparqlQuery(query, namespace, QueryResultFormat.JSON));
//        Response response = blaze.exportFile(RDFFormat.NTRIPLES, namespace, graph);
//        InputStream input = response.readEntity(InputStream.class);
//        Files.copy(input, new File("C:\\RdfData\\ekt_data.nt").toPath());

//        blaze.clearGraphContents(graph, namespace);
//        blaze.importFolder("C:\\RdfData\\EKT RDF\\passed",
//                Utils.fetchDataImportMimeType(RDFFormat.RDFXML),
//                namespace,
//                graph);
//          System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_aa.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_aa.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_ab.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_ac.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_ad.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.importFilePath("C:\\RdfData\\EKT\\ekt_data_ae.nt", RDFFormat.NTRIPLES, namespace, graph).readEntity(String.class));
//        System.out.println(blaze.triplesNum(graph, namespace));
//        System.out.println(blaze.executeUpdateSparqlQuery("insert data {graph <http://test> {<http://a3> <http://p3> <http://b3>.} }",
//                "kb").readEntity(String.class));
//        blaze.deleteNamespace("ekt-demo");
//        blaze.createNamespace(propFile, "ekt-demo");
//        blaze.importFilePath("C:\\RdfData\\res-cidoc_v3.2.1.rdfs", RDFFormat.RDFXML, namespace, "http://cidoc");
//        Utils.saveResponseToFile("C:/RdfData/ekt_cerif_rdf.ttl", blaze.exportFile(RDFFormat.TURTLE, namespace, graph));
    }
}
