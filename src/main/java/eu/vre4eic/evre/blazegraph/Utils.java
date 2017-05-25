/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
        } else if(format == RDFFormat.JSONLD) {
            mimeType = "application/ld+json";
        }
        else {
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
