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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
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
        String namespace = "ekt-data";
//        namespace = "vre-namespace";
        String ektGraph = "http://ekt-data";
        String rcukGraph = "http://rcuk-data";
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
//        blaze.clearGraphContents(rcukGraph, namespace);
//        blaze.clearGraphContents(ektGraph, namespace);
//        blaze.exportFile(RDFFormat.TURTLE, namespace, graph)
//        Response response = blaze.exportFile(RDFFormat.NTRIPLES, namespace, graph);
//        InputStream input = response.readEntity(InputStream.class);
//        blaze.createNamespace(propFile, namespace);

        String folder = "C:\\RdfData\\VREData\\";
//        blaze.importFolder(folder + "RCUK data\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK data\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK data\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK data\\organizations", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
        //////
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\eaddress", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\fundings", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\CERIF RDF data from EKT\\organizationUnits", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        System.out.println("Added: " + (blaze.triplesNum(ektGraph, namespace)));
//        System.out.println("Added: " + (blaze.triplesNum(rcukGraph, namespace)));

//        Response resp = blaze.exportFile(RDFFormat.RDFXML, "ekt-demo", "http://test2");
        Response resp = blaze.executeSparqlQuery("select * where {?s ?p ?o} limit 5", namespace, "text/tab-separated-values");
        System.out.println(resp.readEntity(String.class));
//        Utils.saveResponseToFile(folder + "ekt-data_exported.nt", resp);
    }
}
