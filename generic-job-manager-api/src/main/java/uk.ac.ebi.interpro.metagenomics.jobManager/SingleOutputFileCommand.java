package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A speculative command that specifies a single output file
 * The constructor takes the option that defines the output file
 * ('-o' in example 1 below, '>' in example 2, '-o- in example 3)
 * Examples:
 * hmmsearch --cut_ga -o results.txt sequence.fasta
 * hmmscan --cut_tc sequence.fasta > results.txt
 * hmmsearch sequence.fasta -o results.txt > /dev/null
 *
 * User: craigm
 * Date: 15/10/13
 * Time: 11:35
 * To change this template use File | Settings | File Templates.
 */
public class SingleOutputFileCommand extends SpeculativeCommand{

    private String option;

    public SingleOutputFileCommand(String option, List<String> command ) {
        super(command);
        this.option = option;
        int index = -1;
        String outputFile = "NULL";
        try {
            index = getCommand().indexOf(option);
            outputFile = getCommand().get(index + 1);
            this.outputFiles.add(outputFile);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not identify output file from the supplied option: " + option + "\n" +
                                               "Index = " + index + "\n" +
                                               "Output file = " + outputFile + "\n" +
                                               "Command = " + Arrays.toString(getCommand().toArray()) + "\n" +
                                               e.getClass().toString()       );
        }
    }

    public SingleOutputFileCommand(String option, String... command) {
                this(option, Arrays.asList(command));
     }


    @Override
    public SpeculativeCommand replaceOutputFiles(List<String> newOutputFiles) {
        if (newOutputFiles.size() != 1) {
            throw new IllegalArgumentException("A single new output file should be specified as this command only generates one output file:\n"
                                             + "List of supplied output files: " + newOutputFiles.toArray());
        }
        List<String> newCommand = new ArrayList<String>();
        for (String item: getCommand()) {
            if (outputFiles.contains(item)) {
                newCommand.add(newOutputFiles.get(0));
            } else {
                newCommand.add(item);
            }
        }

        return new SingleOutputFileCommand(option, newCommand);
    }
}
