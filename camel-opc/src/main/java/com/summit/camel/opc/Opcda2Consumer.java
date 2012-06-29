/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.summit.camel.opc;

import java.net.UnknownHostException;
import java.util.Collections;
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

/**
 * The opcda2 consumer.
 */
public class Opcda2Consumer extends ScheduledPollConsumer {

    private final Opcda2Endpoint endpoint;
    private boolean forceHardwareRead;
    private boolean diffOnly;
    private boolean valuesOnly;
    Map<String, Map<String, Object>> previousData = new HashMap<String, Map<String, Object>>();

    public Opcda2Consumer(Opcda2Endpoint endpoint, Processor processor) throws IllegalArgumentException, UnknownHostException, JIException, AlreadyConnectedException {
        super(endpoint, processor);

        super.setDelay(endpoint.getDelay());
        this.endpoint = endpoint;
        diffOnly = endpoint.isDiffOnly();
        valuesOnly = endpoint.isValuesOnly();
        forceHardwareRead = endpoint.isForceHardwareRead();
    }

    @Override
    protected int poll() throws Exception {
        Exchange exchange = endpoint.createExchange();

        Map<String, Map<String, Object>> data = new TreeMap<String, Map<String, Object>>();

        for (String key : endpoint.getOpcItemIds()) {
            Item item = endpoint.getOpcItem(key);
            //TODO this is not serializable... we'll need our own source for this. Dumb.
            ItemState is = item.read(isForceHardwareRead());
            final Map<String, Object> itemStateAsMap = getItemStateAsMap(is);

            if (diffOnly) {
                Map<String, Object> previousItem = previousData.get(key);
                if (previousItem == null) {
                    data.put(key, itemStateAsMap);
                    previousData.put(key, itemStateAsMap);
                } else if (!itemStateAsMap.get("value").equals(previousItem.get("value"))) {
                    data.put(key, itemStateAsMap);
                    previousData.put(key, itemStateAsMap);
                }
            } else if (!diffOnly) {
                data.put(key, itemStateAsMap);
                previousData.put(key, itemStateAsMap);
            }
        }

        exchange.getIn().setBody(data);

        try {
            // send message to next processor in the route
            getProcessor().process(exchange);
            return 1; // number of messages polled
        } finally {
            // log exception if an exception occurred and was not handled
            if (exchange.getException() != null) {
                getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
            }
        }
    }

    private Map<String, Object> getItemStateAsMap(ItemState is) throws JIException {
        Map<String, Object> retVal = new TreeMap<String, Object>();
        if (!isValuesOnly()) {
            retVal.put("errorCode", Integer.valueOf(is.getErrorCode()));
            retVal.put("quality", Short.valueOf(is.getQuality()));
            retVal.put("timestamp", is.getTimestamp());
        }
        retVal.put("value", JIVariantMarshaller.toJavaType(is.getValue()));

        return retVal;
    }

    /**
     * @return the forceHardwareRead
     */
    public boolean isForceHardwareRead() {
        return forceHardwareRead;
    }

    /**
     * @return the valuesOnly
     */
    public boolean isValuesOnly() {
        return valuesOnly;
    }

    /**
     * @param valuesOnly the valuesOnly to set
     */
    public void setValuesOnly(boolean valuesOnly) {
        this.valuesOnly = valuesOnly;
    }
}
