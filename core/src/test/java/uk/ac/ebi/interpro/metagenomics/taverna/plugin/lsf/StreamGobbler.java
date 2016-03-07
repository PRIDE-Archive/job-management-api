package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by matthew on 14/01/14.
 */
public class StreamGobbler extends Thread {

    // StreamGobbler class (to prevent Process.waitFor() method hanging) from:
    // http://www.javaworld.com/article/2071275/core-java/when-runtime-exec---won-t.html

    InputStream is;
    String type;

    StreamGobbler(InputStream is, String type)
    {
        this.is = is;
        this.type = type;
    }

    public void run()
    {
        try
        {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ( (line = br.readLine()) != null)
                System.out.println(type + ">" + line);
            } catch (IOException ioe)
              {
                ioe.printStackTrace();
              }
    }
}
