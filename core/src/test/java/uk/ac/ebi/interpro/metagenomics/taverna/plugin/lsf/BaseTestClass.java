package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf;

import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util.LSFBatchMonitorTest;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 22/08/12
 * Time: 16:39
 * To change this template use File | Settings | File Templates.
 */
public class BaseTestClass {

    public boolean isLSFAvailable() {

        int status = 1;
        try {
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec("bqueues -l");

            // Note: Because some native platforms only provide limited buffer size for standard input and output streams,
            // failure to promptly write the input stream or read the output stream of the subprocess may cause the
            // subprocess to block, and even deadlock.
            // See also http://www.javaworld.com/article/2071275/core-java/when-runtime-exec---won-t.html

            // any error message?
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), "ERROR");

            // any output?
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), "OUTPUT");

            // kick them off
            errorGobbler.start();
            outputGobbler.start();

            // any error???
            status = p.waitFor();
        } catch (Exception e) {
            // if lsf is not available then the process seems to throw an exception
            // rather than return with a non-zero exit status
            return false;
        }
        return status == 0;
    }


    /**
     * Returns the full path to the specified resource file
     * Required for unit tests that need to manipulate these files
     */
    protected String getResourceFilePath(String resourceFileName) {

        URL url = LSFBatchMonitorTest.class.getResource("/" + resourceFileName);
        try {
            URI uri = url.toURI();
            return uri.getPath();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Could not get resource file path", e);  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
