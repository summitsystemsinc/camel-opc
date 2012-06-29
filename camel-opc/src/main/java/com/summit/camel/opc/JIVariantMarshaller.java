/*
 * Copyright 2012 Justin Smith <justin.smith@summitsystemsinc.com>.
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

import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Justin Smith <justin.smith@summitsystemsinc.com>
 */
public class JIVariantMarshaller {
    private static Logger logger = LoggerFactory.getLogger(JIVariantMarshaller.class);
    public static Object toJavaType(JIVariant variant) throws JIException {
        int type = variant.getType();

        switch (type) {
                case JIVariant.VT_I1:
                    //This is a hack... shame on Vikram (Maybe MS doesnt have a byte?)
                    return  Byte.valueOf((byte) variant.getObjectAsChar());
                case JIVariant.VT_I2:
                    return  Short.valueOf(variant.getObjectAsShort());
                case JIVariant.VT_I4:
                    return  Integer.valueOf(variant.getObjectAsInt());
                case JIVariant.VT_I8:
                case JIVariant.VT_INT:
                    return  Long.valueOf(variant.getObjectAsInt());
                case JIVariant.VT_DATE:
                    return  variant.getObjectAsDate();
                case JIVariant.VT_R4:
                    return  Float.valueOf(variant.getObjectAsFloat());
                case JIVariant.VT_R8:
                    return  Double.valueOf(variant.getObjectAsDouble());
                case JIVariant.VT_UI1:
                    return  Byte.valueOf(variant.getObjectAsUnsigned().getValue().byteValue());
                case JIVariant.VT_UI2:
                    return  Short.valueOf(variant.getObjectAsUnsigned().getValue().shortValue());
                case JIVariant.VT_UI4:
                case JIVariant.VT_UINT:
                    return  Integer.valueOf(variant.getObjectAsUnsigned().getValue().intValue());
                case JIVariant.VT_BSTR:
                    return  String.valueOf(variant.getObjectAsString2());
                case JIVariant.VT_ARRAY:
                    return  variant.getObjectAsArray().getArrayInstance();
                case JIVariant.VT_BOOL:
                    return  Boolean.valueOf(variant.getObjectAsBoolean());
                default:
                    logger.warn("Using default case for variant conversion...");
                    return  variant.getObject().toString();
            }
    }
}
