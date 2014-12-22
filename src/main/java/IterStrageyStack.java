package main.java;

import java.io.File;
import java.io.IOException;

import main.java.parser.WorkberchTopologyBuilder;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.iterationstrategy.CrossProduct;
import uk.org.taverna.scufl2.api.iterationstrategy.IterationStrategyNode;
import uk.org.taverna.scufl2.api.iterationstrategy.IterationStrategyStack;
import uk.org.taverna.scufl2.api.iterationstrategy.IterationStrategyTopNode;

public class IterStrageyStack {

	private static String APP_TYPE_TAVERNA_WORKFLOW = "application/vnd.taverna.t2flow+xml"; 
	
	public static void main(final String[] args) {
		final WorkflowBundleIO io = new WorkflowBundleIO();
		final File t2File = new File("application/vnd.taverna.t2flow+xml");

		new WorkberchTopologyBuilder();
		try {

			final WorkflowBundle wfBundle = io.readBundle(t2File, APP_TYPE_TAVERNA_WORKFLOW);

			final Workflow workflow = wfBundle.getMainWorkflow();
			wfBundle.getMainProfile();
			
			final Processor pr = workflow.getProcessors().getByName("Concatenate_two_strings_3");
			
			final IterationStrategyStack iterStrategy = pr.getIterationStrategyStack();
			for (final IterationStrategyTopNode iterNode : iterStrategy) {
				for (final IterationStrategyNode node : iterNode) {
					if (node instanceof CrossProduct) {
						final CrossProduct cp = (CrossProduct) node;
						
						for (final IterationStrategyNode iterationStrategyNode : cp) {
							
						}
					}
				}
			}

		} catch (final ReaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
