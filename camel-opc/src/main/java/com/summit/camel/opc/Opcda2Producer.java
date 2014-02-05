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
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.summit.camel.opc.Opcda2Endpoint.VALUE;
import java.math.BigDecimal;
import java.util.Date;
import org.jinterop.dcom.core.JIArray;
import org.jinterop.dcom.core.JICurrency;
import org.jinterop.dcom.core.JIString;
import org.jinterop.dcom.core.JIVariant;
import org.openscada.opc.lib.da.Item;

/**
 * The opcda2 producer.
 */
public class Opcda2Producer extends DefaultProducer {

    private static final transient Logger LOG = LoggerFactory.getLogger(Opcda2Producer.class);
    private final Opcda2Endpoint endpoint;

    public Opcda2Producer(Opcda2Endpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    /**
     *
     * <b>Sample Exchange:</b>
     * <p>
     * <
     * pre>
     * Exchange[ ExchangePattern: InOnly, BodyType: java.util.TreeMap, Body: {
     * Random.ArrayOfReal8={ value=[Ljava.lang.Double;@52465b94},
     * Random.ArrayOfString={ value=[Ljava.lang.String;@9267bbf},
     * Random.Int1={value=19}, Random.Int2={value=10585},
     * Random.Int4={value=24182}, Random.Money={value=10285},
     * Random.Qualities={value=17}, Random.Real4={value=22136.314},
     * Random.Real8={value=16007.939609460002}, Random.String={value=clients},
     * Random.Time={value=Tue Feb 04 22:05:13 CST 2014},
     * Random.UInt1={value=-33}, Random.UInt2={value=23757},
     * Random.UInt4={value=9832} }]
     * </pre> Its a map, String->Map(String,Object) (value's type may change...)
     * </p>
     *
     * @param exchange
     * @throws Exception
     */
    public void process(Exchange exchange) throws Exception {
        //TODO we need a (optional) custom data type for converters.
        Map<String, Map<String, Object>> data = exchange.getIn().getBody(Map.class);

        for (String tagName : data.keySet()) {
            Object value = data.get(tagName).get(VALUE);

            Item item = endpoint.getOpcItems().get(tagName);
            if (item != null) {
                JIVariant writeValue;
                if (value instanceof Boolean) {
                    writeValue = new JIVariant((Boolean) value);
                } else if (value instanceof Character) {
                    writeValue = new JIVariant((Character) value);
                } else if (value instanceof Date) {
                    writeValue = new JIVariant((Date) value);
                } else if (value instanceof Double) {
                    writeValue = new JIVariant((Double) value);
                } else if (value instanceof Float) {
                    writeValue = new JIVariant((Float) value);
                } else if (value instanceof Integer) {
                    writeValue = new JIVariant((Integer) value);
                } else if (value instanceof String) {
                    writeValue = new JIVariant((String) value);
                } else if (value instanceof Byte) {
                    writeValue = new JIVariant((Byte) value);
                } else if (value instanceof Short) {
                    writeValue = new JIVariant((Short) value);
                    //TODO we need a class for currency...
                } else if (value instanceof BigDecimal) {
                    writeValue = new JIVariant(new JICurrency(((BigDecimal) value).toPlainString()));
                } else if (value instanceof Long) {
                    writeValue = new JIVariant((Long) value);
                    //TODO Array's, Currency
                } else if (value instanceof Object[]) {
                    /*
                     System.out.println(tagName);
                     writeValue = new JIVariant(new JIArray(value));
                     */
                    log.info("Arrays not currently supported.");
                    writeValue = new JIVariant(JIVariant.NULL());
                } else {
                    throw new CamelOpcException(String.format("Data Type not supported: %s", value.getClass()));
                }
                item.write(writeValue);
            } else {
                if (endpoint.isFailIfTagAbsent()) {
                    throw new CamelOpcException(String.format("Tag %s not found.", tagName));
                }
            }
        }
    }

}
