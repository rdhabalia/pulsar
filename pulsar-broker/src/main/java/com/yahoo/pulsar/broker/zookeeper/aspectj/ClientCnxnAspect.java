/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.zookeeper.aspectj;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetMaxChildrenResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetSASLResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncResponse;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class ClientCnxnAspect {

    private static final int writeType = 0;
    private static final int readType = 1;
    private static final int otherType = 2;

    private static final Map<Class<? extends Record>, String> responseMap = new HashMap<Class<? extends Record>, String>() {
        {
            put(CreateResponse.class, CreateResponse.class.getSimpleName());
            put(SetDataResponse.class, CreateResponse.class.getSimpleName());
        }
    };

    @Pointcut("execution(void org.apache.zookeeper.ClientCnxn.processEvent(..))")
    public void processEvent() {
    }

    @Around("processEvent()")
    public void timedProcessEvent(ProceedingJoinPoint joinPoint) throws Throwable {
        joinPoint.proceed();

        long startTimeMs = getStartTime(joinPoint.getArgs()[0]);
        if (startTimeMs == -1) {
            // couldn't find start time
            return;
        }
        Record response = getEventResponse(joinPoint.getArgs()[0]);
        if (response != null) {
            int type = checkType(response);
            if (type == writeType) {
                System.out.println("writetype = " + (MathUtils.now() - startTimeMs));
            } else if (type == writeType) {
                System.out.println("readType = " + (MathUtils.now() - startTimeMs));
            } else {
                System.out.println("other = " + (MathUtils.now() - startTimeMs));
            }
        }
    }

    private int checkType(Record response) {

        if (response == null) {
            return otherType;
        } else if (response instanceof CreateResponse) {
            return writeType;
        } else if (response instanceof SetDataResponse) {
            return writeType;
        } else if (response instanceof SetDataResponse) {
            return writeType;
        } else if (response instanceof SyncResponse) {
            return writeType;
        } else if (response instanceof SetSASLResponse) {
            return writeType;
        } else if (response instanceof ConnectResponse) {
            return writeType;
        } else if (response instanceof SetWatches) {
            return writeType;
        } else if (response instanceof GetDataResponse) {
            return readType;
        } else if (response instanceof GetChildrenResponse) {
            return readType;
        } else if (response instanceof ExistsResponse) {
            return readType;
        } else if (response instanceof ErrorResponse) {
            return readType;
        } else if (response instanceof GetACLResponse) {
            return readType;
        } else if (response instanceof GetChildren2Response) {
            return readType;
        } else if (response instanceof GetChildrenResponse) {
            return readType;
        } else if (response instanceof GetMaxChildrenResponse) {
            return readType;
        }
        return otherType;
    }

    private long getStartTime(Object packet) {
        try {
            if (packet.getClass().getName().equals("org.apache.zookeeper.ClientCnxn$Packet")) {
                Field ctxField = Class.forName("org.apache.zookeeper.ClientCnxn$Packet").getDeclaredField("ctx");
                ctxField.setAccessible(true);
                Object zooworker = ctxField.get(packet);
                if (zooworker.getClass().getName().equals("org.apache.bookkeeper.zookeeper.ZooWorker")) {
                    Field timeField = Class.forName("org.apache.bookkeeper.zookeeper.ZooWorker")
                            .getDeclaredField("startTimeMs");
                    timeField.setAccessible(true);
                    long startTime = (long) timeField.get(zooworker);
                    return startTime;
                }
            }
        } catch (Exception e) {
            // Ok
        }
        return -1;
    }

    private Record getEventResponse(Object packet) {
        try {
            if (packet.getClass().getName().equals("org.apache.zookeeper.ClientCnxn$Packet")) {
                Field field = Class.forName("org.apache.zookeeper.ClientCnxn$Packet").getDeclaredField("response");
                field.setAccessible(true);
                Record response = (Record) field.get(packet);
                return response;
            }
        } catch (Exception e) {
            // Ok
        }

        return null;
    }

}
