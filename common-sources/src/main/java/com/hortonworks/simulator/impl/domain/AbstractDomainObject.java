package com.hortonworks.simulator.impl.domain;

import akka.actor.UntypedActor;
import com.hortonworks.simulator.interfaces.DomainObject;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.logging.Logger;

public abstract class AbstractDomainObject extends UntypedActor implements DomainObject,
		Serializable {
	private static final long serialVersionUID = -2630503054916573455L;
	protected Logger logger = Logger.getLogger(this.getClass().toString());

	@Override
	public String toString() {
		return new ToStringBuilder(this).toString();
	}
}
