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
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.camel.Consumer;
import org.apache.camel.EndpointConfiguration;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.jinterop.dcom.common.JIException;
import org.openscada.opc.lib.common.AlreadyConnectedException;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.AddFailedException;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.Server;
import org.openscada.opc.lib.da.browser.Branch;
import org.openscada.opc.lib.da.browser.Leaf;
import org.openscada.opc.lib.da.browser.TreeBrowser;

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
    private int delay = 500;
    private Server opcServer;
    private Group opcGroup;
    private boolean diffOnly = false;
    private boolean valuesOnly = true;
    private Map<String, Item> opcItems = new TreeMap<String, Item>();
    
    private boolean forceHardwareRead = false;

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
        initializeServerConnection();
        return new Opcda2Producer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        initializeServerConnection();
        Opcda2Consumer retVal = new Opcda2Consumer(this, processor);
        return retVal;
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
    private void initializeServerConnection() throws OPCConnectionException {
        if (getOpcServer() == null) {

            if (getClsId() == null && getProgId() == null) {
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

            try {
                opcServer.connect();

                opcGroup = opcServer.addGroup(getId());
            } catch (Exception ex) {
                throw new OPCConnectionException(ex.getMessage(), ex);
            }
            EndpointConfiguration cfg = getEndpointConfiguration();
            String opcTreePath = cfg.getParameter("path");
            String[] pathArray = opcTreePath.split("/");
            try {
                TreeBrowser treeBrowser = opcServer.getTreeBrowser();
                Branch root = treeBrowser.browse();
                Branch parent = root;

                for (int i = 0; i < pathArray.length; i++) {
                    boolean found = false;
                    //This should handle "//" and the first /
                    if(pathArray[i].isEmpty()){
                        continue;
                    }
                    for (Branch candidate : parent.getBranches()) {
                        if (candidate.getName().equals(pathArray[i])) {
                            parent = candidate;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new OPCConnectionException("Unable to find sub-group: " + pathArray[i]);
                    }
                }
                populateItemsMapRecursive(parent);
            } catch (Exception ex) {
                throw new OPCConnectionException(ex.getMessage(), ex);
            }
        }
    }

    public void populateItemsMapRecursive(Branch parent) throws JIException, AddFailedException {
        for (Leaf l : parent.getLeaves()) {
            String itemId = l.getItemId();
            Item i = opcGroup.addItem(itemId);
            opcItems.put(itemId, i);
        }
        for(Branch child : parent.getBranches()){
            populateItemsMapRecursive(child);
        }
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

    /**
     * @return the forceHardwareRead
     */
    public boolean isForceHardwareRead() {
        return forceHardwareRead;
    }

    /**
     * @param forceHardwareRead the forceHardwareRead to set
     */
    public void setForceHardwareRead(boolean forceHardwareRead) {
        this.forceHardwareRead = forceHardwareRead;
    }

    /**
     * @return the delay
     */
    public int getDelay() {
        return delay;
    }

    /**
     * @param delay the delay to set
     */
    public void setDelay(int delay) {
        this.delay = delay;
    }

    /**
     * @return the diffOnly
     */
    public boolean isDiffOnly() {
        return diffOnly;
    }

    /**
     * @param diffOnly the diffOnly to set
     */
    public void setDiffOnly(boolean diffOnly) {
        this.diffOnly = diffOnly;
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

    private final class Opcda2EndpointThreadFactory implements ThreadFactory {

        int count = 0;

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, Opcda2Endpoint.this.getId() + "_" + count);
            return t;
        }
    }

    public Collection<String> getOpcItemIds() {
        return opcItems.keySet();
    }

    public Collection<Item> getOpcItems() {
        return opcItems.values();
    }

    public Item getOpcItem(String itemId) {
        return opcItems.get(itemId);
    }
}
