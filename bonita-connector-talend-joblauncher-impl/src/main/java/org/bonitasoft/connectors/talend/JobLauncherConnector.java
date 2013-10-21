/**
 * Copyright (C) 2009 BonitaSoft S.A.
 * BonitaSoft, 31 rue Gustave Eiffel - 38000 Grenoble
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2.0 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.bonitasoft.connectors.talend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.connector.AbstractConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;

import routines.system.api.TalendJob;

/**
 * 
 * @author Charles Souillard
 * @author Arthur Freycon
 * 
 */
public class JobLauncherConnector extends AbstractConnector {

	private static final String RUN_JOB_METHOD = "runJob";

	private static final String CONTEXT_PARAM_PREFIX = "--context_param:";

	private static final String CONTEXT_PARAM_SEPARATOR = "=";

	private static final String PROJECT_NAME = "projectName";

	private static final String JOB_NAME = "jobName";

	private static final String JOB_VERSION = "jobVersion";

	private static final String JOB_PARAMETERS = "jobParameters";

	private static final String PRINT_OUTPUT = "printOutput";

	private static final String BUFFER_OUTPUT = "bufferOutput";

	// IN
	private String projectName;

	private String jobName;

	private String jobVersion;

	private Map<String, String> jobParameters;

	private boolean printOutput;

	// OUT
	private java.lang.String[][] bufferOutput;

	public java.lang.String[][] getBufferOutput() {
		return (String[][]) getOutputParameters().get(BUFFER_OUTPUT);
	}

	@Override
	protected void executeBusinessLogic() throws ConnectorException {
		final String jobClassName = projectName.toLowerCase() + "." + jobName.toLowerCase() + "_" + jobVersion.replace('.', '_') + "." + jobName;

		Class<?> clazz = null;
		try{
			clazz = Thread.currentThread().getContextClassLoader().loadClass(jobClassName);
		}catch(ClassNotFoundException cnfe){
			throw new ConnectorException("The TalendJob class "+jobClassName+" has not been found in the process classpath.");
		}

		try{
			final TalendJob jobInstance = (TalendJob) clazz.newInstance();

			Collection<String> jobParams = new ArrayList<String>();
			if (jobParameters != null) {
				for (Map.Entry<String, String> parameter : jobParameters.entrySet()) {
					jobParams.add(CONTEXT_PARAM_PREFIX + parameter.getKey() + CONTEXT_PARAM_SEPARATOR + parameter.getValue());
				}
			}

			bufferOutput = jobInstance.runJob(jobParams.toArray(new String[] {}));
			setOutputParameter(BUFFER_OUTPUT, bufferOutput);

			if (printOutput) {
				printBufferOutput();
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		// use case : use talend to get all products list: first column in french, second in english. TO be mapped to a combo box for example
		// add an input to say where output must be stored
	}

	@Override
	public void validateInputParameters() throws ConnectorValidationException {
		projectName = (String) getInputParameter(PROJECT_NAME);
		jobName = (String) getInputParameter(JOB_NAME);
		jobVersion = (String) getInputParameter(JOB_VERSION);
		jobParameters = setJobParameters((List<List<Object>>) getInputParameter(JOB_PARAMETERS));
		printOutput = (Boolean) getInputParameter(PRINT_OUTPUT);

		List<String> errors = new ArrayList<String>();

		if (projectName == null || projectName.trim().length() == 0) {
			errors.add("projectName cannot be empty");
		}
		if (jobName == null || jobName.trim().length() == 0) {
			errors.add("jobName cannot be empty");
		}
		if (jobVersion == null || jobVersion.trim().length() == 0) {
			errors.add("jobVersion cannot be empty");
		}

		if (!errors.isEmpty()) {
			throw new ConnectorValidationException(this, errors);
		}
	}

	private void printBufferOutput() {
		System.out.println("Buffer output for job: " + jobName + " " + jobVersion + " (project=" + projectName + "):");
		int line = 0;
		for (String[] columns : getBufferOutput()) {
			System.out.print("Line " + line + ": ");
			int column = 0;
			for (String s : columns) {
				System.out.print(s);
				if (column < (columns.length - 1)) {
					System.out.print(", ");
				}
				column++;
			}
			System.out.println();
			line++;
		}
	}

	private Map<String, String> setJobParameters(final List<List<Object>> jobParametersList) {
		Map<String, String> jobParametersMap = new HashMap<String, String>();
		if (jobParametersList != null) {
			for (List<Object> rows : jobParametersList) {
				if (rows.size() == 2) {
					Object keyContent = rows.get(0);
					Object valueContent = rows.get(1);
					if (keyContent != null && valueContent != null) {
						final String key = keyContent.toString();
						final String value = valueContent.toString();
						jobParametersMap.put(key, value);
					}
				}
			}
		}
		return jobParametersMap;
	}

}
