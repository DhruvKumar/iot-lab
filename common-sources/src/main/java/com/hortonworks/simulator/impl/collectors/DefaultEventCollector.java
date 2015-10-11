package com.hortonworks.simulator.impl.collectors;

import com.hortonworks.simulator.impl.domain.AbstractEventCollector;

public class DefaultEventCollector extends AbstractEventCollector {

	
	@Override
	public void onReceive(Object message) throws Exception {
		logger.info(message);
	}


}
