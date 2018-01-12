
import eu.vre4eic.evre.blazegraph.BlazegraphRepRestful;
import gr.forth.ics.ChangeDetection;
import gr.forth.ics.Datamanager;
import gr.forth.ics.Setting;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.openrdf.rio.RDFFormat;
import org.slf4j.LoggerFactory;

/*
 * Copyright 2017 rousakis.
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
/**
 *
 * @author rousakis
 */
public class BlazegraphTest {

    public static void main(String[] args) throws UnsupportedEncodingException, IOException, Exception {
        Set<String> loggers = new HashSet<>(Arrays.asList(
                "org.openrdf.rio",
                "org.apache.http",
                "groovyx.net.http",
                "org.eclipse.jetty.client",
                "org.eclipse.jetty.io",
                "org.eclipse.jetty.http",
                "o.e.jetty.util",
                "o.e.j.u.component",
                "org.openrdf.query.resultio"));
        for (String log : loggers) {
            ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(log);
            logger.setLevel(ch.qos.logback.classic.Level.INFO);
            logger.setAdditive(false);
        }

        BlazegraphRepRestful blaze = new BlazegraphRepRestful("http://139.91.183.40:9999/blazegraph", "detection");
        Datamanager data = new Datamanager();
        LinkedHashMap<String, String> versions = data.initOntological();
        versions = data.initMultidim();
        List<String> versionGraphs = new ArrayList(versions.keySet());
        //////
        for (String graph : versions.keySet()) {
//            blaze.clearGraphContents(graph);
//            blaze.importFilePath(versions.get(graph), RDFFormat.N3, graph);
        }
        //////
        String changesFolder = Setting.getChangesPath(Setting.BLAZEGRAPH_ONTOLOGICAL);
        changesFolder = Setting.getChangesPath(Setting.BLAZEGRAPH_MULTIDIM);
        String changesOntology = "http://changes";
        for (int i = 1; i < versionGraphs.size(); i++) {
            String oldVersion = versionGraphs.get(i - 1);
            String newVersion = versionGraphs.get(i);
            detectChanges(changesFolder, oldVersion, newVersion, changesOntology, blaze);
        }

    }

    private static void detectChanges(String changesFolder, String oldVersion, String newVersion, String changesOntology, BlazegraphRepRestful blaze) throws Exception, UnsupportedEncodingException {
        List<String> changes = ChangeDetection.getConsideredChanges(changesFolder, oldVersion, newVersion, changesOntology);
        System.out.println("-------------");
        System.out.println("Simple Change Detection among versions:");
        System.out.println(oldVersion + " (" + blaze.triplesNum(oldVersion) + " triples)");
        System.out.println(newVersion + " (" + blaze.triplesNum(newVersion) + " triples)");
        System.out.println("-------------");
        long oldSize = blaze.triplesNum(changesOntology);
        System.out.print("Detecting simple changes...");
        long start = System.currentTimeMillis();
        for (String query : changes) {
            blaze.executeSparqlUpdateQuery(query);
        }
        System.out.println("DONE in " + (System.currentTimeMillis() - start));
        System.out.println("Simple change triples number: " + (blaze.triplesNum(changesOntology) - oldSize));
        blaze.clearGraphContents(changesOntology);
    }

}
