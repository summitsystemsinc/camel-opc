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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.jinterop.dcom.common.JIException;
import org.openscada.opc.lib.common.AlreadyConnectedException;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.Server;

/**
 * Represents a opcda2 endpoint.
 */
public class Opcda2Endpoint extends DefaultEndpoint {

    private String domain = "localhost";
    private String host = "localhost";
    private String clsId;
    private String progId;
    private String username;
    private String password;
    private int poolSize = 2;
    private Server opcServer;

    public Opcda2Endpoint() {
    }

    public Opcda2Endpoint(String uri, Opcda2Component component) {
        super(uri, component);
    }

    public Opcda2Endpoint(String endpointUri) {
        super(endpointUri);
    }

    @Override
    public Producer createProducer() throws Exception {
        return new Opcda2Producer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return new Opcda2Consumer(this, processor);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * @return the domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * @param domain the domain to set
     */
    public void setDomain(String domain) {
        this.domain = domain;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }
    
    protected Server getOpcServer() {
        return opcServer;
    }    
    
    /**
     * 
     * @return A connection to this endpoints opc server.
     * @throws IllegalArgumentException
     * @throws UnknownHostException
     * @throws JIException
     * @throws AlreadyConnectedException 
     */
    public Server getServerConnection() throws IllegalArgumentException, UnknownHostException, JIException, AlreadyConnectedException{
        if (getOpcServer() == null) {
            if(getClsId() == null && getProgId() == null){
                throw new OPCConnectionException("clsId OR progId MUST BE SET!");
            }
            ConnectionInformation connInfo = new ConnectionInformation();
            
            connInfo.setClsid(getClsId());
            connInfo.setProgId(getProgId());
            connInfo.setHost(getHost());
            connInfo.setDomain(getDomain());
            connInfo.setUser(getUsername());
            connInfo.setPassword(getPassword());
            
            ScheduledExecutorService execService =
                    Executors.newScheduledThreadPool(
                    getPoolSize(),
                    new Opcda2EndpointThreadFactory());
            
            opcServer = new Server(connInfo, execService);
            opcServer.connect();
        }

        return opcServer;
    }

    /**
     * @return the poolSize
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @param poolSize the poolSize to set
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * @return the clsId
     */
    public String getClsId() {
        return clsId;
    }

    /**
     * @param clsId the clsId to set
     */
    public void setClsId(String clsId) {
        this.clsId = clsId;
    }

    /**
     * @return the progId
     */
    public String getProgId() {
        return progId;
    }

    /**
     * @param progId the progId to set
     */
    public void setProgId(String progId) {
        this.progId = progId;
    }

    private final class Opcda2EndpointThreadFactory implements ThreadFactory {

        int count = 0;

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, Opcda2Endpoint.this.getId() + "_" + count);
            return t;
        }
    }
}
