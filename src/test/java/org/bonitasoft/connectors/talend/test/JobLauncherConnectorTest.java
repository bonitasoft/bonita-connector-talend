package org.bonitasoft.connectors.talend.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;

import org.bonitasoft.connectors.talend.JobLauncherConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.Test;

public class JobLauncherConnectorTest {

    private static final String PROJECT_NAME = "projectName";

    private static final String JOB_NAME = "jobName";

    private static final String JOB_VERSION = "jobVersion";

    private static final String JOB_PARAMETERS = "jobParameters";

    private static final String PRINT_OUTPUT = "printOutput";

    private static File expectedFile = null;

    @Test(expected = ConnectorValidationException.class)
    public void testEmptyProjectName() throws Exception {
        Map<String, Object> alteredParams = new HashMap<String, Object>();
        alteredParams.put(PROJECT_NAME, null);
        JobLauncherConnector jobLauncher = getConnector(alteredParams);
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
    }

    @Test(expected = ConnectorValidationException.class)
    public void testEmptyJobName() throws Exception {
        Map<String, Object> alteredParams = new HashMap<String, Object>();
        alteredParams.put(JOB_NAME, "");
        JobLauncherConnector jobLauncher = getConnector(alteredParams);
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
    }

    @Test(expected = ConnectorValidationException.class)
    public void testEmptyJobVersion() throws Exception {
        Map<String, Object> alteredParams = new HashMap<String, Object>();
        alteredParams.put(JOB_VERSION, " ");
        JobLauncherConnector jobLauncher = getConnector(alteredParams);
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
    }

    @Test(expected = ConnectorException.class)
    public void testWrongJobName() throws Exception {
        Map<String, Object> alteredParams = new HashMap<String, Object>();
        alteredParams.put(JOB_NAME, "WrongJob");
        JobLauncherConnector jobLauncher = getConnector(alteredParams);
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
    }

    @Test
    public void testTalendJobLauncher() throws Exception {
        JobLauncherConnector jobLauncher = getConnector(new HashMap<String, Object>());
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
        // assertTrue("Generated file must exists after the talend job is executed: " + expectedFile, expectedFile.exists());
        // assertTrue("The generated file must contain many lines with at least one containing James;", checkContains(expectedFile, ";"));
        String[][] bufferOutput = jobLauncher.getBufferOutput();
        assertEquals("0", bufferOutput[0][0]);
        expectedFile.delete();
    }

    @Test
    public void testTalendJobLauncherWithOutput() throws Exception {
        Map<String, Object> alteredParams = new HashMap<String, Object>();
        alteredParams.put(PRINT_OUTPUT, true);
        JobLauncherConnector jobLauncher = getConnector(alteredParams);
        jobLauncher.validateInputParameters();
        jobLauncher.execute();
        String[][] actual = jobLauncher.getBufferOutput();
        String[][] expected = new String[][] { { "0" } };
        for (int i = 0; i < expected.length; i++) {
            for (int j = 0; j < expected[0].length; j++) {
                assertEquals(expected[i][j], actual[i][j]);
            }
        }
        // assertTrue("Generated file must exists after the talend job is executed: " + expectedFile, expectedFile.exists());
        // assertTrue("The generated file must contain many lines with at least one containing James;", checkContains(expectedFile, ";"));
        // assertTrue("The generated file must contain many lines with at least one containing James;", checkContains(expectedFile, "James"));
        expectedFile.delete();
    }

    private JobLauncherConnector getConnector(Map<String, Object> alteredParams) {
        String directory = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath() + File.separator;
        String fileName = null;
        do {
            fileName = "talendOutput" + new Random().nextInt(1000000);
            expectedFile = new File(directory + fileName + ".csv");
            expectedFile.deleteOnExit();
        } while (expectedFile.exists());

        List<String> jobParameterListElem1 = new ArrayList<String>(2);
        jobParameterListElem1.add("new1");
        jobParameterListElem1.add(directory);
        List<String> jobParameterListElem2 = new ArrayList<String>(2);
        jobParameterListElem2.add("new2");
        jobParameterListElem2.add(fileName);
        List<List<String>> jobParametersList = new ArrayList<List<String>>(2);
        jobParametersList.add(jobParameterListElem1);
        jobParametersList.add(jobParameterListElem2);

        JobLauncherConnector jobLauncher = new JobLauncherConnector();
        Map<String, Object> paramsMap = new HashMap<String, Object>();
        paramsMap.put(PROJECT_NAME, "projectname");
        paramsMap.put(JOB_NAME, "jobTalend52");
        paramsMap.put(JOB_VERSION, "0.1");
        paramsMap.put(JOB_PARAMETERS, jobParametersList);
        paramsMap.put(PRINT_OUTPUT, false);
        if (alteredParams != null) {
            for (Entry<String, Object> entry : alteredParams.entrySet()) {
                paramsMap.put(entry.getKey(), entry.getValue());
            }
        }
        jobLauncher.setInputParameters(paramsMap);
        assertFalse("Generated file must not exists before the talend job is executed: " + expectedFile, expectedFile.exists());
        return jobLauncher;
    }

    public boolean checkContains(File sourceFile, String s) throws FileNotFoundException {
        Scanner scanner = new Scanner(sourceFile);
        try {
            while (scanner.hasNextLine()) {
                if (scanner.nextLine().contains(s)) {
                    return true;
                }
            }
            return false;
        } finally {
            scanner.close();
        }
    }
}
