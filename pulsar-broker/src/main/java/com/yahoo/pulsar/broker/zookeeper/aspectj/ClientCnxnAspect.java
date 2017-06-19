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
import org.apache.zookeeper.proto.*;
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
        System.out.println("I am called");
        joinPoint.proceed();

        long startTimeMs = getStartTime(joinPoint.getArgs()[0]);
        if (startTimeMs == -1) {
            // couldn't find start time
            return;
        }
        Record request = getEventType(joinPoint.getArgs()[0]);
        System.out.println(request.getClass());
        if (request != null) {
            int type = checkType(request);
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
        } else if (response instanceof ConnectRequest) {
            return writeType;
        } else if (response instanceof CreateRequest) {
            return writeType;
        } else if (response instanceof DeleteRequest) {
            return writeType;
        } else if (response instanceof SetDataRequest) {
            return writeType;
        } else if (response instanceof SetACLRequest) {
            return writeType;
        } else if (response instanceof SetMaxChildrenRequest) {
            return writeType;
        } else if (response instanceof SetSASLRequest) {
            return writeType;
        } else if (response instanceof SetWatches) {
            return writeType;
        } else if (response instanceof SyncRequest) {
            return writeType;
        } else if (response instanceof ExistsRequest) {
            return readType;
        } else if (response instanceof GetDataRequest) {
            return readType;
        } else if (response instanceof GetMaxChildrenRequest) {
            return readType;
        } else if (response instanceof GetACLRequest) {
            return readType;
        } else if (response instanceof GetChildrenRequest) {
            return readType;
        } else if (response instanceof GetChildren2Request) {
            return readType;
        } else if (response instanceof GetSASLRequest) {
            return readType;
        } else {
            return otherType;
        }
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

    private Record getEventType(Object packet) {
        try {
            if (packet.getClass().getName().equals("org.apache.zookeeper.ClientCnxn$Packet")) {
                Field field = Class.forName("org.apache.zookeeper.ClientCnxn$Packet").getDeclaredField("request");
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
