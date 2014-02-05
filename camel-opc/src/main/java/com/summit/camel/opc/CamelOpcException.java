/*
 * Copyright 2014 justin.
 *
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
 */

package com.summit.camel.opc;

/**
 *
 * @author justin
 */
public class CamelOpcException extends Exception {

    /**
     * Creates a new instance of <code>CamelOpcException</code> without detail
     * message.
     */
    public CamelOpcException() {
    }

    /**
     * Constructs an instance of <code>CamelOpcException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public CamelOpcException(String msg) {
        super(msg);
    }
    
}
