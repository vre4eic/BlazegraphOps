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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author rousakis
 */
public class RousTest {

    public static void main(String[] args) throws UnsupportedEncodingException, IOException, Exception {
        Set<String> loggers = new HashSet<>(Arrays.asList("org.apache.http",
                "org.openrdf.query.resultio",
                "org.openrdf.rio",
                "org.eclipse.jetty.util",
                "org.eclipse.jetty.util.component",
                "org.eclipse.jetty.io",
                "org.eclipse.jetty.client.util",
                "org.eclipse.jetty.client",
                "org.eclipse.jetty.http"));
        for (String log : loggers) {
            Logger logger = (Logger) LoggerFactory.getLogger(log);
            logger.setLevel(Level.INFO);
            logger.setAdditive(false);
        }
        String propFile = "/config/quads.properties";
        String service = "http://139.91.183.46:9999/blazegraph"; //seistro
        service = "http://139.91.183.70:9999/blazegraph"; //seistro2
//        service = "http://139.91.183.40:9999/blazegraph"; //stalone
//        service = "http://83.212.97.61:9999/blazegraph";  //edet
        service = "http://139.91.183.97:9999/blazegraph"; //celsius
//        service = "http://83.212.99.102:9999/blazegraph"; //edet modip
        String namespace = "vre4eic";
//        namespace = "vre4eictest";
//        namespace = "geotest";
        String ektGraph = "http://ekt-data";
        String rcukGraph = "http://rcuk-data";
        String frisGraph = "http://fris-data";
        String eposGraph = "http://epos-data";
        String envriGraph = "http://envri-data";
        String servicesGraph = "http://epos-data-services";
        BlazegraphRepRestful blaze = new BlazegraphRepRestful(service);
//        blaze.clearGraphContents(ektGraph, namespace);
//        blaze.deleteNamespace(namespace);
//        blaze.createNamespaceFromResources(propFile, namespace);

        String folder = "E:\\RdfData\\VREData\\";
//        blaze.clearGraphContents(rcukGraph, namespace);
        blaze.importFilePath(folder + "services(1).rdf", RDFFormat.RDFXML, namespace, servicesGraph);
//        blaze.importFolder(folder + "RCUK\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
////        blaze.importFolder(folder + "RCUK\\organizations", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFolder(folder + "RCUK\\organizations_with_synthetic_geo_data", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, rcukGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, envriGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, envriGraph);
//        blaze.importFilePath(folder + "RCUK\\classification1.ntriples", RDFFormat.NTRIPLES, namespace, rcukGraph);
        //////
//        blaze.importFolder(folder + "EKT RDF\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\eaddress", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\fundings", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\organizationUnits", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF\\organizations_with_synthetic_geo_data", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, ektGraph);

//        blaze.importFolder(folder + "EKT RDF\\postalAddresses", Utils.fetchDataImportMimeType(RDFFormat.N3), namespace, ektGraph);
//        blaze.importFolder(folder + "EKT RDF", Utils.fetchDataImportMimeType(RDFFormat.TURTLE), namespace, ektGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, ektGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, ektGraph);
//        blaze.importFilePath(folder + "EKT RDF\\classification1.ntriples", RDFFormat.NTRIPLES, namespace, ektGraph);
        ///
//        blaze.clearGraphContents(frisGraph, namespace);
//        blaze.importFolder(folder + "FRIS\\organizations", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\persons", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\projects", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFolder(folder + "FRIS\\publications", Utils.fetchDataImportMimeType(RDFFormat.NTRIPLES), namespace, frisGraph);
//        blaze.importFilePath(folder + "classification.ttl", RDFFormat.TURTLE, namespace, frisGraph);
//        blaze.importFilePath(folder + "5providers.rdf", RDFFormat.RDFXML, namespace, frisGraph);
//        blaze.importFilePath(folder + "FRIS\\classification1.ntriples", RDFFormat.NTRIPLES, namespace, frisGraph);
        ///////
//        blaze.clearGraphContents(eposGraph, namespace);
////        blaze.clearGraphContents(envriGraph, namespace);
//        blaze.importFilePath(folder+"cerif1.6h-good.ttl", RDFFormat.TURTLE, namespace, eposGraph);
//        blaze.importFilePath(folder+"classification.ttl", RDFFormat.TURTLE, namespace, eposGraph);
//        blaze.importFolder(folder + "EPOS", Utils.fetchDataImportMimeType(RDFFormat.RDFXML), namespace, eposGraph);
//        blaze.importFilePath("C:\\RdfData\\VREData\\cerif1.6h-good.ttl", RDFFormat.TURTLE, namespace, envriGraph);
//        blaze.importFilePath("C:\\RdfData\\VREData\\classification.ttl", RDFFormat.TURTLE, namespace, envriGraph);
//        blaze.importFolder(folder + "ENVRIplus", Utils.fetchDataImportMimeType(RDFFormat.RDFXML), namespace, envriGraph);
//        System.out.println(blaze.importFilePath("geo_epos.nt", RDFFormat.NTRIPLES, namespace, eposGraph));
//        System.out.println(blaze.importFilePath("geo_envri.nt", RDFFormat.NTRIPLES, namespace, envriGraph));
//        
//        System.out.println(blaze.importFilePath("date_epos.nt", RDFFormat.NTRIPLES, namespace, eposGraph));
//        System.out.println(blaze.importFilePath("date_envri.nt", RDFFormat.NTRIPLES, namespace, envriGraph));
//        System.out.println(blaze.importFilePath("date_fris.nt", RDFFormat.NTRIPLES, namespace, frisGraph));
//        System.out.println(blaze.importFilePath("date_rcuk.nt", RDFFormat.NTRIPLES, namespace, rcukGraph));
//        System.out.println(blaze.importFilePath("date_ekt.nt", RDFFormat.NTRIPLES, namespace, ektGraph));
        ////
//        Response resp = blaze.exportFile(RDFFormat.NTRIPLES, "epos-data", null);
//        Response resp = blaze.executeSparqlQuery("select * where {?s ?p ?o} limit 5", namespace, "text/tab-separated-values");
//        System.out.println(resp.readEntity(String.class));
//        Utils.saveResponseToFile(folder + "epos-data_exported.nt", resp);
//        calcStats(blaze, eposGraph, namespace);
//        calcStats(blaze, envriGraph, namespace);
//
//        System.out.println(blaze.importFilePath("blaze_geodata\\geo_ekt.nt", RDFFormat.NTRIPLES, namespace, ektGraph));
//        System.out.println(blaze.importFilePath("blaze_geodata\\geo_rcuk.nt", RDFFormat.NTRIPLES, namespace, rcukGraph));
//        System.out.println(blaze.importFilePath("blaze_geodata\\geo_fris.nt", RDFFormat.NTRIPLES, namespace, frisGraph));
//        System.out.println(blaze.importFilePath("blaze_geodata\\geo_epos.nt", RDFFormat.NTRIPLES, namespace, eposGraph));
//        System.out.println(blaze.importFilePath("blaze_geodata\\geo_envri.nt", RDFFormat.NTRIPLES, namespace, envriGraph));
//
//        blaze.clearGraphContents("http://vre/classifications", namespace);
//        System.out.println(blaze.importFilePath("C:\\Users\\rousakis\\AppData\\Roaming\\Skype\\My Skype Received Files\\classifications\\CERIF_VRE4EIC_Terms.ntriples",
//                RDFFormat.NTRIPLES, namespace, "http://vre/classifications"));
//        System.out.println(blaze.importFilePath("C:\\Users\\rousakis\\AppData\\Roaming\\Skype\\My Skype Received Files\\classifications\\classification.ntriples",
//                RDFFormat.NTRIPLES, namespace, "http://vre/classifications"));

        String query = "select ?service ?base ?response ?status ?request_parameter ?style ?type ?method from <http://services-data>\n"
                + "\n"
                + "where {\n"
                + "\n"
                + "?service_uri a <http://www.cidoc-crm.org/cidoc-crm/Service> .\n"
                + "?service_uri <http://www.w3.org/2000/01/rdf-schema#label> ?service .\n"
                + "\n"
                + "?component_uri <http://www.cidoc-crm.org/cidoc-crm/has_service> ?service_uri .\n"
                + "?component_uri <http://www.w3.org/2000/01/rdf-schema#label> ?component . \n"
                + "\n"
                + "?component_uri <http://www.cidoc-crm.org/cidoc-crm/has_base> ?base_uri .\n"
                + "?base_uri <http://www.w3.org/2000/01/rdf-schema#label> ?base .\n"
                + "\n"
                + "?service_uri <http://www.cidoc-crm.org/cidoc-crm/has_method> ?method_uri .\n"
                + "?method_uri <http://www.w3.org/2000/01/rdf-schema#label> ?method .\n"
                + "\n"
                + "?method_uri <http://www.cidoc-crm.org/cidoc-crm/has_response> ?response_uri .\n"
                + "?response_uri <http://www.w3.org/2000/01/rdf-schema#label> ?response .\n"
                + "\n"
                + "\n"
                + "OPTIONAL{\n"
                + "\n"
                + "?response_uri <http://www.cidoc-crm.org/cidoc-crm/has_status> ?status_uri .\n"
                + "?status_uri <http://www.w3.org/2000/01/rdf-schema#label> ?status.\n"
                + "\n"
                + "}\n"
                + "\n"
                + "OPTIONAL{\n"
                + "\n"
                + "?method_uri <http://www.cidoc-crm.org/cidoc-crm/has_request_parameter> ?parameter_uri .\n"
                + "?parameter_uri <http://www.w3.org/2000/01/rdf-schema#label> ?request_parameter .\n"
                + "\n"
                + "?parameter_uri <http://www.cidoc-crm.org/cidoc-crm/has_style> ?style_uri .\n"
                + "?style_uri <http://www.w3.org/2000/01/rdf-schema#label> ?style.\n"
                + "\n"
                + "?parameter_uri <http://www.cidoc-crm.org/cidoc-crm/has_type> ?type_uri .\n"
                + "?type_uri <http://www.w3.org/2000/01/rdf-schema#label> ?type .\n"
                + "\n"
                + "}\n"
                + "\n"
                + "} order by ?service";
//        Response resp = blaze.executeSparqlQuery(query, namespace, QueryResultFormat.CSV);
//        System.out.println(resp.readEntity(String.class));
//        
//        BlazegraphRepRemote blazeRem = new BlazegraphRepRemote("", service);
//        TupleQueryResult res = blazeRem.executeSPARQLQuery(query, namespace);
//        HashMap<String, String> results = new HashMap<>();
//        while (res.hasNext()) {
//            BindingSet set = res.next();
//            String relation = URLDecoder.decode(set.getValue("relation").stringValue(), "UTF-8");
//            String relatedEntity = URLDecoder.decode(set.getValue("relation").stringValue(), "UTF-8");
//            results.put(relation.substring(relation.lastIndexOf("/") + 1), relatedEntity);
//        }
//        System.out.println(results);
//        blazeRem.terminate();
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
