/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils.tests;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import java.util.Properties;

/**
 *
 * @author rousakis
 */
public class HelloBlazegraph {

    public static void main(String[] args) throws OpenRDFException {

        final Properties props = new Properties();
        props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
        props.put(Options.FILE, "/tmp/blazegraph/test.jnl"); // journal file location

        final BigdataSail sail = new BigdataSail(props); // instantiate a sail
        final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository

        repo.initialize();

        try {
            // prepare a statement
            URIImpl subject = new URIImpl("http://blazegraph.com/Blazegraph");
            URIImpl predicate = new URIImpl("http://blazegraph.com/says");
            Literal object = new LiteralImpl("hello");
            Statement stmt = new StatementImpl(subject, predicate, object);

            // open repository connection
            RepositoryConnection cxn = repo.getConnection();

            // upload data to repository
            try {
                cxn.begin();
                cxn.add(stmt);
                cxn.commit();
            } catch (OpenRDFException ex) {
                cxn.rollback();
                throw ex;
            } finally {
                // close the repository connection
                cxn.close();
            }

            // open connection
            if (repo instanceof BigdataSailRepository) {
                cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
            } else {
                cxn = repo.getConnection();
            }

            // evaluate sparql query
            try {

                final TupleQuery tupleQuery = cxn
                        .prepareTupleQuery(QueryLanguage.SPARQL,
                                "select ?p ?o where { <http://blazegraph.com/Blazegraph> ?p ?o . }");
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    while (result.hasNext()) {
                        BindingSet bindingSet = result.next();
                        System.err.println(bindingSet);
                    }
                } finally {
                    result.close();
                }

            } finally {
                // close the repository connection
                cxn.close();
            }

        } finally {
            repo.shutDown();
        }
    }
}
