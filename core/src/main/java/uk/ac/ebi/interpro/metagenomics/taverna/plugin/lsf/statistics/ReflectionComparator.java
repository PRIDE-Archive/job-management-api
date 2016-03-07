package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import java.lang.reflect.Method;
import java.util.Comparator;

/**
 * Order a collection by the supplied field name.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: ReflectionComparator.java,v 1.1 2013/10/18 10:39:41 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class ReflectionComparator implements Comparator {

    // This class took inspiration from:
    // http://www.coderanch.com/t/382093/java/java/Sorting-ArrayList-MemberBean-objects

    // TODO Note: This class isn't actually used in the project. But not deleted since it might be useful one day!

    private String methodName;

    public ReflectionComparator(String methodName) {
        this.methodName = methodName;
    }

    public int compare(Object o1, Object o2) {

        try {
            Method m1 = o1.getClass().getMethod(methodName, new Class[] {});
            Method m2 = o2.getClass().getMethod(methodName, new Class[] {});
            return ((String) m1.invoke(o1, new Object[] {})).compareTo((String) m2.invoke(o2, new Object[] {}));
        }
        catch (Exception ex) {
            throw new RuntimeException("A problem occurred comparing " + methodName, ex);
        }

    }
}
