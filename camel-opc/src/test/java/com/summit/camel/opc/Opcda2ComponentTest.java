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

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

public class Opcda2ComponentTest extends CamelTestSupport {

    String domain;
    String user;
    String password;
    String clsid;
    String host;
    Properties props;

    @Test
    public void testopcda2() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMinimumMessageCount(5);

        log.info("Domain: " + domain);
        log.info("User: " + user);
        log.info("Password: " + password);
        log.info("Host: " + host);
        log.info("ClsId: " + clsid);

        assertMockEndpointsSatisfied();
        log.info("Got " + mock.getExpectedCount() + " exchanges.");
    }

    private void initProperties() throws Exception{
        
        props = new Properties();
        props.load(Opcda2ComponentTest.class.getResourceAsStream("opcServer.properties"));

        File localPropsFile = new File(System.getProperty("user.home") + "/.camel-opc-test", "opcServer.properties");
        
        if (localPropsFile.exists()) {
            props.load(new FileInputStream(localPropsFile));
        }
        
        domain = props.getProperty("opc.domain");
        user = props.getProperty("opc.user");
        password = props.getProperty("opc.password");
        host = props.getProperty("opc.host");
        clsid = props.getProperty("opc.clsid");

        if (domain == null || user == null || password == null || host == null || clsid == null) {
            //TODO hint at the fields.
            throw new Exception("All opc settings must be populated to run this test!");
        }
    }
    
    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        initProperties();
        
        return new RouteBuilder() {

            @Override
            public void configure() {
                //TODO externalize this to the properties file.
                String uriString = "opcda2:opcdaTest/Simulation Items/Bucket Brigade?delay=500&host=" + host + "&clsId=" + clsid + "&username=" + user + "&password=" + password + "&domain=" + domain;

                from(uriString).to("log:OPC_Test?level=info").to("mock:result");
            }
        };
    }
}
