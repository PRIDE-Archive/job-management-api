package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 14/08/12
 * Time: 10:56
 * To change this template use File | Settings | File Templates.
 */
public class Utils {

    /**
     * Reads {@link InputStream} line by line and converts it into a {@link String}.
     *
     * @param is Input stream. Always close at the end.
     * @return
     */
    public static String getStandardOutput(InputStream is) {
        StringBuilder sb = new StringBuilder();
        BufferedInputStream buffer = new BufferedInputStream(is);
        BufferedReader reader = new BufferedReader(new InputStreamReader(buffer));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (sb.length() > 0) {
                    sb.append("\n");
                }
                sb.append(line);

            }
            reader.close();
            is.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return sb.toString();
    }
}