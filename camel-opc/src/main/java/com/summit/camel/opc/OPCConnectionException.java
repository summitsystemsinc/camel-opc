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


/**
 *
 * @author <a href="mailto:justin.smith@summitsystemsinc.com">Justin Smith</a>
 */
public class OPCConnectionException extends Exception {

    public OPCConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OPCConnectionException(String message) {
        super(message);
    }

    public OPCConnectionException() {
    }

    public OPCConnectionException(Throwable cause) {
        super(cause);
    }
}
