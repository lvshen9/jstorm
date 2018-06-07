/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.callback;

import com.google.common.annotations.VisibleForTesting;
import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Time;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.SmartThread;

/**
 * wraps timer thread to execute afn every several seconds, if an exception is thrown, run killFn
 *
 * @author yannian
 */
public class AsyncLoopThread implements SmartThread {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncLoopThread.class);

    private Thread thread;
    private RunnableCallback afn;

    public AsyncLoopThread(RunnableCallback afn) {
        this.init(afn, false, Thread.NORM_PRIORITY, true);
    }

    public AsyncLoopThread(RunnableCallback afn, boolean daemon, int priority, boolean start) {
        this.init(afn, daemon, priority, start);
    }

    /**
     *
     * @param afn              RunnableCallback  需要使用线程异步循环往复执行的任务
     * @param daemon           是否作为守护线程执行，默认为false
     * @PARAM kill_fn          指定执行杀死任务的任务，默认值为AsyncLoopDefaultKill
     * @param priority         线程的优先级,  默认:Thread.NORM_PRIORITY
     * @param start            是否即刻启动该线程，默认值为true
     */
    public AsyncLoopThread(RunnableCallback afn, boolean daemon, RunnableCallback kill_fn, int priority, boolean start) {
        this.init(afn, daemon, kill_fn, priority, start);
    }

    public void init(RunnableCallback afn, boolean daemon, int priority, boolean start) {
        RunnableCallback kill_fn = new AsyncLoopDefaultKill();
        this.init(afn, daemon, kill_fn, priority, start);
    }

    private void init(RunnableCallback afn, boolean daemon, RunnableCallback kill_fn, int priority, boolean start) {
        if (kill_fn == null) {
            kill_fn = new AsyncLoopDefaultKill();
        }
        //组合任务：要执行的主任务，负责杀死主任务的任务
        Runnable runnable = new AsyncLoopRunnable(afn, kill_fn);

        //给任务创建线程
        thread = new Thread(runnable);
        String threadName = afn.getThreadName();
        if (threadName == null) {
            threadName = afn.getClass().getSimpleName();
        }
        //给线程设定相关属性：线程名，守护线程，优先级
        thread.setName(threadName);
        thread.setDaemon(daemon);
        thread.setPriority(priority);
        thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("UncaughtException", e);
                JStormUtils.halt_process(1, "UncaughtException");
            }
        });

        this.afn = afn;

        if (start) {
            //立即启动该线程，执行组合任务。
            thread.start();
        }
    }

    @Override
    public void start() {
        thread.start();
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    @VisibleForTesting
    public void join(int times) throws InterruptedException {
        thread.join(times);
    }

    @Override
    public void interrupt() {
        thread.interrupt();
    }

    @Override
    public Boolean isSleeping() {
        return Time.isThreadWaiting(thread);
    }

    public Thread getThread() {
        return thread;
    }

    @Override
    public void cleanup() {
        afn.shutdown();
    }
}
