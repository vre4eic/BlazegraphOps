/*
 * Copyright 2018 rousakis.
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.LoggerFactory;

/**
 *
 * @author rousakis
 */
public class GeospatialGenerator {

    public static void main(String[] args) throws UnsupportedEncodingException, Exception {
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
        String service = "http://139.91.183.46:9999/blazegraph"; //seistro
//        service = "http://139.91.183.70:9999/blazegraph"; //seistro2
//        service = "http://139.91.183.40:9999/blazegraph"; //stalone
//        service = "http://83.212.97.61:9999/blazegraph";  //edet
        service = "http://139.91.183.97:9999/blazegraph"; //celsius
//        service = "http://83.212.99.102:9999/blazegraph"; //edet modip
        String namespace = "vre4eic";
        String graph = "http://fris-data";
        String filename = "fris.nt";

//        createDatesFile(service, graph, namespace, "date_" + filename);
        createLocationsFile(service, graph, namespace, "geo_" + filename);
//        long maxDate = new Date(Long.MAX_VALUE).getTime();

    }

    private static void createDatesFile(String service, String graph, String namespace, String filename) throws IOException, Exception {
        BlazegraphRepRemote blaze = new BlazegraphRepRemote(service);
        String query = "select * from <" + graph + "> where {\n"
                + "  ?sub <http://eurocris.org/ontology/cerif#has_startDate> ?start.\n"
                + "  ?sub <http://eurocris.org/ontology/cerif#has_endDate> ?end.\n"
                + "}";
        TupleQueryResult result = blaze.executeSPARQLQuery(query, namespace);
        BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        while (result.hasNext()) {
            StringBuilder sb = new StringBuilder();
            BindingSet set = result.next();
            String sub = set.getValue("sub").stringValue();
            String start = set.getValue("start").stringValue();
            String end = set.getValue("end").stringValue();
            if (!start.contains("T")) {
                start += "T00:00:00";
            }
            if (!end.contains("T")) {
                end += "T00:00:00";
            }
            Date dateStart = sdf.parse(start);
            Date dateEnd = sdf.parse(end);
//            System.out.println(start + " -> " + dateStart.getTime());
//            System.out.println(end + " -> " + dateEnd.getTime());
            sb.append("<" + sub + "> <http://date/start> \"0#0#" + dateStart.getTime() + "\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon-time>.\n");
            sb.append("<" + sub + "> <http://date/end> \"0#0#" + dateEnd.getTime() + "\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon-time>.\n");
            bw.append(sb.toString());
        }
        result.close();
        bw.close();
        blaze.terminate();
    }

    private static void createLocationsFile(String service, String graph, String namespace, String filename) throws IOException, Exception {
        BlazegraphRepRemote blaze = new BlazegraphRepRemote(service);
        String query = "select ?GBB ?north_west ?south_west ?north_east ?south_east from <" + graph + "> where {\n"
                + "  ?GBB <http://eurocris.org/ontology/cerif#has_northBoundaryLatitude> ?northLat.\n"
                + "  ?GBB <http://eurocris.org/ontology/cerif#has_southBoundaryLatitude> ?southLat.\n"
                + "  ?GBB <http://eurocris.org/ontology/cerif#has_eastBoundaryLongitude> ?eastLong.\n"
                + "  ?GBB <http://eurocris.org/ontology/cerif#has_westBoundaryLongitude> ?westLong.\n"
                + "  #\n"
                + "  bind (concat('\\\"',?northLat,'#',?westLong,'\\\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon>') as ?north_west).\n"
                + "  bind (concat('\\\"',?southLat,'#',?westLong,'\\\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon>') as ?south_west).\n"
                + "  bind (concat('\\\"',?northLat,'#',?eastLong,'\\\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon>') as ?north_east).\n"
                + "  bind (concat('\\\"',?southLat,'#',?eastLong,'\\\"^^<http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon>') as ?south_east).\n"
                + "} ";
        TupleQueryResult result = blaze.executeSPARQLQuery(query, namespace);
        BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
        while (result.hasNext()) {
            StringBuilder sb = new StringBuilder();
            BindingSet set = result.next();
            String gbb = set.getValue("GBB").stringValue();
            String nw = set.getValue("north_west").stringValue();
            String sw = set.getValue("south_west").stringValue();
            String ne = set.getValue("north_east").stringValue();
            String se = set.getValue("south_east").stringValue();
            sb.append("<" + gbb + "> <http://location/north-west> " + nw + ".\n");
            sb.append("<" + gbb + "> <http://location/south-west> " + sw + ".\n");
            sb.append("<" + gbb + "> <http://location/north-east> " + ne + ".\n");
            sb.append("<" + gbb + "> <http://location/south-east> " + se + ".\n");
            bw.append(sb.toString());
        }
        result.close();
        bw.close();
        blaze.terminate();
    }
}
