/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.trace

import groovy.transform.CompileStatic
import groovy.json.JsonOutput
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.BroadcastOperations;



/**
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 * @author Mike Smoot <mes@aescon.com>
 */
@CompileStatic
class TraceSocketioObserver implements TraceObserver {

    private final BroadcastOperations bo;
    private final SocketIOServer server;

    TraceSocketioObserver() {
        Configuration config = new Configuration();
        config.setHostname("localhost");
        config.setPort(9092);

        server = new SocketIOServer(config);
        bo = server.getBroadcastOperations();
    }

    /**
     * The is method is invoked when the flow is going to start
     */
    @Override
    void onFlowStart(Session session) {
        server.start();
        bo.sendEvent("message", getJson("onFlowStart", []));
    }

    /**
     * This method is invoked when the flow is going to complete
     */
    @Override
    void onFlowComplete() {
        bo.sendEvent("message", getJson("onFlowStop", []));
        server.stop();
    }

    /*
     * Invoked when the process is created.
     */
    @Override
    void onProcessCreate( TaskProcessor process ) {
        bo.sendEvent("message", getJson("onProcessCreate", []));
    }

    /**
     * This method is invoked before a process run is going to be submitted
     * @param handler
     */
    @Override
    void onProcessSubmit(TaskHandler handler) {
        bo.sendEvent("message", getJson("onProcessSubmit", handler));
    }

    /**
     * This method is invoked when a process run is going to start
     * @param handler
     */
    @Override
    void onProcessStart(TaskHandler handler) {
        bo.sendEvent("message", getJson("onProcessStart", handler));
    }

    /**
     * This method is invoked when a process run completes
     * @param handler
     */
    @Override
    void onProcessComplete(TaskHandler handler) {
        bo.sendEvent("message", getJson("onProcessComplete", handler));
    }

    /**
     * method invoked when a task execution is skipped because a cached result is found
     * @param handler
     */
    @Override
    void onProcessCached(TaskHandler handler) {
        bo.sendEvent("message", getJson("onProcessCached", handler));
    }

    private String getJson(messageName, handler) {
        def rec = []

        if ( handler instanceof TaskHandler ) {
            rec = handler.getTraceRecord().store
        }

        return JsonOutput.toJson(["name": messageName, "record": rec])
    }

}
