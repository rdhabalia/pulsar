/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.zookeeper.aspectj;

import java.util.Set;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import com.google.common.collect.Sets;

@Aspect
public class LoggerAspect {

    private static final Set<String> tokens = Sets.newHashSet();

    @Pointcut("execution(void org.slf4j.impl.*.debug(..))")
    public void processDebug() {
    }

    @Around("processDebug()")
    public void debugProcess(ProceedingJoinPoint joinPoint) throws Throwable {
        if (tokens.isEmpty()) {
            joinPoint.proceed();
            return;
        }
        processDebugLog(joinPoint);
    }

    private void processDebugLog(ProceedingJoinPoint joinPoint) throws Throwable {
        boolean found = false;
        Object[] arguments = joinPoint.getArgs();
        for (int i = 1; i < arguments.length; i++) {
            if (arguments[i] instanceof String) {
                for (String token : tokens) {
                    if (((String) arguments[i]).contains(token)) {
                        found = true;
                        break;
                    }
                }
            }
        }
        if (found) {
            joinPoint.proceed();
        }
    }

    public static void addDebugToken(String token) {
        tokens.add(token);
    }

    public static void removeDebugToken(String token) {
        tokens.remove(token);
    }

    public static void clearTokens() {
        tokens.clear();
    }
}