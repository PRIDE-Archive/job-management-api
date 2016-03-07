package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.SpeculativeCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 16/10/13
 * Time: 11:03
 * To change this template use File | Settings | File Templates.
 */
public class LSFSpeculativeJob extends LSFJob{



    private final String SPECULATIVE_EXTENSION = ".SPEC";
    private final LSFBsubCommand speculativeBsubCommand;
    private final LSFJob speculativeLSFJob;
    private final List<String> speculativeFilesToMove = new ArrayList<String>();
    private final List<String> destinationFiles = new ArrayList<String>();
    private String speculativeProjectName = null;


    public LSFSpeculativeJob(LSFSpeculativeBsubCommand jobSchedulerCommand) {
        super(jobSchedulerCommand);
        LSFSpeculativeBsubCommand originalBsubCommand = (LSFSpeculativeBsubCommand)this.getJobSchedulerCommand();
        SpeculativeCommand systemCommand = (SpeculativeCommand)originalBsubCommand.getSystemCmd();
        List<String> originalOutputFiles = systemCommand.getOutputFiles();
        List<String> speculativeOutputFiles = generateSpeculativeOutputFiles(originalOutputFiles);
        SpeculativeCommand newCommand = systemCommand.replaceOutputFiles(speculativeOutputFiles);
        Map<LSFBsubOption, Object> speculativeLsfOptions = generateSpeculativeLSFOptions(originalBsubCommand.getOptions());
        speculativeBsubCommand = new LSFSpeculativeBsubCommand(newCommand, speculativeLsfOptions);
        speculativeLSFJob = new LSFJob(speculativeBsubCommand);
    }

    private List<String> generateSpeculativeOutputFiles(List<String> originalOutputFiles) {
        List<String> speculativeOutputFiles = new ArrayList<String>();
        for (String originalOutputFile: originalOutputFiles) {
            String speculativeOutputFile = originalOutputFile + SPECULATIVE_EXTENSION;
            speculativeOutputFiles.add(speculativeOutputFile);
            speculativeFilesToMove.add(speculativeOutputFile);
            destinationFiles.add(originalOutputFile);
        }
        return speculativeOutputFiles;
    }

    private Map<LSFBsubOption, Object> generateSpeculativeLSFOptions(Map<LSFBsubOption, Object> originalLSFOptions) {
        Map<LSFBsubOption, Object> newLSFOptions = new HashMap<LSFBsubOption, Object>();
        for (LSFBsubOption option: originalLSFOptions.keySet()) {
            switch(option) {
                case JOB_NAME:
                    newLSFOptions.put(LSFBsubOption.JOB_NAME,
                            (String) originalLSFOptions.get(LSFBsubOption.JOB_NAME) + SPECULATIVE_EXTENSION);
                    break;
                case PROJECT_NAME:
                    newLSFOptions.put(LSFBsubOption.PROJECT_NAME,
                            (String) originalLSFOptions.get(LSFBsubOption.PROJECT_NAME) + SPECULATIVE_EXTENSION);
                    speculativeProjectName = (String) originalLSFOptions.get(LSFBsubOption.PROJECT_NAME) + SPECULATIVE_EXTENSION;
                case STD_OUTPUT_FILE:
                    String originalOutputFile = (String) originalLSFOptions.get(LSFBsubOption.STD_OUTPUT_FILE);
                    String newOutputFile =  originalOutputFile + SPECULATIVE_EXTENSION;
                    newLSFOptions.put(LSFBsubOption.STD_OUTPUT_FILE, newOutputFile);
                    speculativeFilesToMove.add(newOutputFile);
                    destinationFiles.add(originalOutputFile);
                    break;
                case STD_ERROR_OUTPUT_FILE:
                    String originalErrFile = (String)  originalLSFOptions.get(LSFBsubOption.STD_ERROR_OUTPUT_FILE);
                    String newErrFile = originalErrFile + SPECULATIVE_EXTENSION;
                    newLSFOptions.put(LSFBsubOption.STD_ERROR_OUTPUT_FILE, newErrFile);
                    speculativeFilesToMove.add(newErrFile);
                    destinationFiles.add(originalErrFile);
                    break;
                default:
                    newLSFOptions.put(option, originalLSFOptions.get(option));
                    break;
            }

        }
        return newLSFOptions;

    }

    public LSFJob getAlternateJob() {
        return speculativeLSFJob;
    }

    public String getSpeculativeProjectName() {
        return speculativeProjectName;
    }

    public List<String> getSpeculativeFilesToMove() {
        return speculativeFilesToMove;
    }
    public List<String> getDestinationFiles() {
        return destinationFiles;
    }

}
