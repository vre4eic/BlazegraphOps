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
//        service = "http://83.212.97.61:9999/blazegraph";  //edet
        service = "http://139.91.183.97:9999/blazegraph"; //celsius
        String namespace = "vre4eic";
//        namespace = "ekt-data";
        String ektGraph = "http://ekt-data";
        String rcukGraph = "http://rcuk-data";
        String frisGraph = "http://fris-data";
        String eposGraph = "http://epos-data";
        String envriGraph = "http://envri-data";
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
//        blaze.clearGraphContents(rcukGraph, namespace);
//        blaze.clearGraphContents(ektGraph, namespace);
//        blaze.createNamespace(propFile, namespace);
        String folder = "C:\\RdfData\\VREData\\";
//        blaze.clearGraphContents(rcukGraph, namespace);
//        blaze.importFolder(folder + "RCUK\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\organizations", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, rcukGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, rcukGraph);
        //////
//        blaze.importFolder(folder + "EKT RDF\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\eaddress", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\fundings", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\organizationUnits", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF", Utils.fetchDataImportMimeType(RDFFormat.TURTLE), namespace, ektGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, ektGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, ektGraph);
        ///
//        blaze.clearGraphContents(frisGraph, namespace);
//        blaze.importFolder(folder + "FRIS\\organizations", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, frisGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, frisGraph);
        ///////
////        blaze.clearGraphContents(eposGraph, namespace);
////        blaze.clearGraphContents(envriGraph, namespace);
//        blaze.importFilePath("C:\\RdfData\\VREData\\cerif1.6h-good.ttl", RDFFormat.TURTLE, namespace, eposGraph);
//        blaze.importFilePath("C:\\RdfData\\VREData\\cerif1.6h-good.ttl", RDFFormat.TURTLE, namespace, envriGraph);
//        blaze.importFilePath("C:\\RdfData\\VREData\\classification.ttl", RDFFormat.TURTLE, namespace, eposGraph);
//        blaze.importFilePath("C:\\RdfData\\VREData\\classification.ttl", RDFFormat.TURTLE, namespace, envriGraph);
//        blaze.importFolder(folder + "ENVRIplus", Utils.fetchDataImportMimeType(RDFFormat.RDFXML), namespace, envriGraph);
//        blaze.importFolder(folder + "EPOS", Utils.fetchDataImportMimeType(RDFFormat.RDFXML), namespace, eposGraph);
        ////
//        Response resp = blaze.exportFile(RDFFormat.NTRIPLES, "epos-data", null);
//        Response resp = blaze.executeSparqlQuery("select * where {?s ?p ?o} limit 5", namespace, "text/tab-separated-values");
//        System.out.println(resp.readEntity(String.class));
//        Utils.saveResponseToFile(folder + "epos-data_exported.nt", resp);
//        calcStats(blaze, eposGraph, namespace);
//        calcStats(blaze, envriGraph, namespace);
    }

    private static void calcStats(BlazegraphRepRestful blaze, String ektGraph, String namespace) throws Exception {
        System.out.println("Triples: " + blaze.countSparqlResults("select * from <" + ektGraph + "> where {\n"
                + "  ?s ?p ?o.\n"
                + "}", namespace));
        System.out.println("Organisation Units: " + blaze.countSparqlResults("select * from <" + ektGraph + "> where {\n"
                + "  ?s a <http://eurocris.org/ontology/cerif#OrganisationUnit>.\n"
                + "}", namespace));
        System.out.println("Projects: " + blaze.countSparqlResults("select * from <" + ektGraph + "> where {\n"
                + "  ?s a <http://eurocris.org/ontology/cerif#Project>.\n"
                + "}", namespace));
        System.out.println("Persons: " + blaze.countSparqlResults("select * from <" + ektGraph + "> where {\n"
                + "  ?s a <http://eurocris.org/ontology/cerif#Person>.\n"
                + "}", namespace));
        System.out.println("Publications: " + blaze.countSparqlResults("select * from <" + ektGraph + "> where {\n"
                + "  ?s a <http://eurocris.org/ontology/cerif#Publication>.\n"
                + "}", namespace));
    }

}
