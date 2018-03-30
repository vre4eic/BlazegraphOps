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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author rousakis
 */
public class Utils {

    public static String fetchDataImportMimeType(RDFFormat format) {
        String mimeType;
        if (format == RDFFormat.RDFXML) {
            mimeType = "application/rdf+xml";
        } else if (format == RDFFormat.N3) {
            mimeType = "text/rdf+n3";
        } else if (format == RDFFormat.NTRIPLES) {
            mimeType = "text/plain";
        } else if (format == RDFFormat.TURTLE) {
            mimeType = "application/x-turtle";
        } else if (format == RDFFormat.JSONLD) {
            mimeType = "application/ld+json";
        } else if (format == RDFFormat.TRIG) {
            mimeType = "application/x-trig";
        } else if (format == RDFFormat.NQUADS) {
            mimeType = "text/x-nquads";
        } else if (format == RDFFormat.JSONLD) {
            mimeType = "application/ld+json";
        } else {
            mimeType = null;
        }
        return mimeType;
    }

    public static String fetchQueryResultMimeType(QueryResultFormat format) {
        String mimetype = "";
        switch (format) {
            case CSV:
                mimetype = "text/csv";
                break;
            case JSON:
                mimetype = "application/json";
                break;
            case TSV:
                mimetype = "text/tab-separated-values";
                break;
            case XML:
                mimetype = "application/sparql-results+xml";
                break;
        }
        return mimetype;
    }

    public static QueryResultFormat QueryResultFormatfromString(String format) {
        if (format != null) {
            for (QueryResultFormat b : QueryResultFormat.values()) {
                if (format.equalsIgnoreCase(b.toString())) {
                    return b;
                }
            }
        }
        return null;
    }

    public static RDFFormat RDFFormatfromString(String format) {
        if (format != null) {

            for (RDFFormat b : RDFFormat.values()) {
                if (format.equalsIgnoreCase(b.toString())) {
                    return b;
                }
            }
        }
        return null;
    }

    public static boolean saveResponseToFile(String filename, Response resp) {
        InputStream input = resp.readEntity(InputStream.class);
        new File(filename).delete();
        try {
            Files.copy(input, new File(filename).toPath());
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

}
