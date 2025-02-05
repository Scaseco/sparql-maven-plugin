package org.aksw.maven.plugin.qlever;

import org.aksw.commons.util.derby.DerbyUtils;
import org.aksw.commons.util.exception.FinallyRunAll;
import org.aksw.commons.util.function.ThrowingRunnable;
import org.apache.jena.sys.JenaSystem;
import org.apache.maven.plugin.MojoExecutionException;

public class JenaMojoHelper {
	public static final String xmlInputFactoryKey = "javax.xml.stream.XMLInputFactory";

	/**
	 * Wrapper that
	 * <ul>
	 *   <li>configures Jena XML processing</li>
	 * </ul>
	 */
    public static void execJenaBasedMojo(ThrowingRunnable runnable) throws MojoExecutionException {
    	DerbyUtils.disableDerbyLog();
    	
    	// Setting the property is needed to avoid error-level log messages from XSD validation
    	// See: https://issues.apache.org/jira/browse/JENA-2331
    	String xmlInputFactoryValue = System.getProperty(xmlInputFactoryKey);
    	System.setProperty(xmlInputFactoryKey, "com.sun.xml.internal.stream.XMLInputFactoryImpl");    	
    	
    	JenaSystem.init();
    	try {
    		runnable.run();
    	} catch (Exception e) {
            throw new MojoExecutionException("Error encountered during plugin execution", e);
    	} finally {
            // Need to manually shutdown apache sis to avoid the following exception:
            //  Exception in thread "Shutdown" java.lang.NoClassDefFoundError: org/apache/sis/metadata/sql/util/LocalDataSource$1
            //  Caused by: java.lang.ClassNotFoundException: org.apache.sis.metadata.sql.util.LocalDataSource$1
    		FinallyRunAll.run(
    			// () -> Shutdown.stop(JenaMojoHelper.class),
    			() -> {
    				if (xmlInputFactoryValue == null) {
    					System.clearProperty(xmlInputFactoryKey);
    				} else {
    					System.setProperty(xmlInputFactoryKey, xmlInputFactoryValue);
    				}
    			}
    		);
    	}
    }
}