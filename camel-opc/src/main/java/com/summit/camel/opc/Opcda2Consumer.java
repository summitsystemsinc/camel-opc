package com.summit.camel.opc;

/*
 * #%L
 * Camel-OPC :: OPCDA 2 Component
 * %%
 * Copyright (C) 2013 - 2014 Summit Management Systems, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.jinterop.dcom.common.JIException;
import org.openscada.opc.lib.common.AlreadyConnectedException;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.ItemState;

import static com.summit.camel.opc.Opcda2Endpoint.VALUE;
import static com.summit.camel.opc.Opcda2Endpoint.TIMESTAMP;
import static com.summit.camel.opc.Opcda2Endpoint.QUALITY;
import static com.summit.camel.opc.Opcda2Endpoint.ERROR_CODE;

/**
 * The opcda2 consumer.
 *
 * @author <a href="mailto:justin.smith@summitsystemsinc.com">Justin Smith</a>
 */
public class Opcda2Consumer extends ScheduledPollConsumer {

	private final Opcda2Endpoint endpoint;
	Map<String, Map<String, Object>> previousData = new HashMap<String, Map<String, Object>>();

	public Opcda2Consumer(Opcda2Endpoint endpoint, Processor processor) throws IllegalArgumentException, UnknownHostException, JIException, AlreadyConnectedException {
		super(endpoint, processor);

		super.setDelay(endpoint.getDelay());
		this.endpoint = endpoint;
	}

	@Override
	protected int poll() throws Exception {
		Exchange exchange = endpoint.createExchange();

		Map<String, Map<String, Object>> data = new TreeMap<String, Map<String, Object>>();
		final Map<String, Item> opcItems = endpoint.getOpcItems();

		for (String key : opcItems.keySet()) {
			Item item = opcItems.get(key);
			//TODO this is not serializable... we'll need our own source for this. Dumb.
			ItemState is = item.read(endpoint.isForceHardwareRead());
			final Map<String, Object> itemStateAsMap = getItemStateAsMap(is);
			final boolean diffOnly = endpoint.isDiffOnly();

			if (diffOnly) {
				Map<String, Object> previousItem = previousData.get(key);
				if (previousItem == null) {
					data.put(key, itemStateAsMap);
					previousData.put(key, itemStateAsMap);
				} else {
					final Object newValue = itemStateAsMap.get("value");
					final Object oldValue = previousItem.get("value");
					boolean diff = false;

					if (newValue instanceof Object[] && oldValue instanceof Object[]) {
						if (!Arrays.equals((Object[]) newValue, (Object[]) oldValue)) {
							diff = true;
						}
					} else if (!newValue.equals(oldValue)) {
						diff = true;
					}
					if (diff) {
						data.put(key, itemStateAsMap);
						previousData.put(key, itemStateAsMap);
					}
				}
			} else if (!diffOnly) {
				data.put(key, itemStateAsMap);
				previousData.put(key, itemStateAsMap);
			}
		}

		exchange.getIn().setBody(data);

		try {
			// send message to next processor in the route
			if (!data.isEmpty()) {
				getProcessor().process(exchange);
				return 1; // number of messages polled
			} else {
				return 0;
			}
		} finally {
			// log exception if an exception occurred and was not handled
			if (exchange.getException() != null) {
				getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
			}
		}
	}

	private Map<String, Object> getItemStateAsMap(ItemState is) throws JIException {
		Map<String, Object> retVal = new TreeMap<String, Object>();
		if (!endpoint.isValuesOnly()) {
			retVal.put(ERROR_CODE, Integer.valueOf(is.getErrorCode()));
			retVal.put(QUALITY, Short.valueOf(is.getQuality()));
			retVal.put(TIMESTAMP, is.getTimestamp().getTime());
		}
		retVal.put(VALUE, JIVariantMarshaller.toJavaType(is.getValue()));

		return retVal;
	}
}
