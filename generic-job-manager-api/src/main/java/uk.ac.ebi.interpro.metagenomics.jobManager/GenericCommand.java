package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a command as a list of Strings, as the {@link ProcessBuilder} consumes a list of Strings.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: GenericCommand.java,v 1.3 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public abstract class GenericCommand implements Serializable {

    private List<String> command;

    //for serialization
    protected GenericCommand() {}


    protected GenericCommand(List<String> command) {
        this.command = command;
    }

    public List<String> getCommand() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericCommand)) return false;

        GenericCommand that = (GenericCommand) o;

        if (command != null ? !command.equals(that.command) : that.command != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return command != null ? command.hashCode() : 0;
    }
}