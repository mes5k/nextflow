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
import java.nio.file.Path

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor

import com.corundumstudio.socketio.Configuration
import com.corundumstudio.socketio.SocketIOServer
import com.corundumstudio.socketio.BroadcastOperations

/**
 * SocketIO trace event broadcast server.
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 * @author Mike Smoot <mes@aescon.com>
 */
@Slf4j
class BroadcastObserver implements TraceObserver {

    private final SocketIOServer server;
    private final BroadcastOperations broadcast;

    BroadcastObserver( /* int port */ ) {
        log.info 'Constructing Broadcast observer'
        Configuration config = new Configuration();
        config.setHostname('localhost');
        config.setPort(9092);

        server = new SocketIOServer(config);
        broadcast = server.getBroadcastOperations();
    }

    @Override
    void onFlowStart(Session session) {
        server.start();
        def msg = "{'type': 'flow_start', 'name': 'pipeline'}"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
    }

    @Override
    void onFlowComplete() {
        def msg = "{'type': 'flow_stop', 'name': 'pipeline'}"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
        server.stop();
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        def msg = "{'type': 'process_create', 'name': '${process.getName()}'}"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
    }


    @Override
    void onProcessSubmit(TaskHandler handler) {
        def msg = "{'type': 'process_submit', 'name': '${handler.task.name}' }"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
    }

    @Override
    void onProcessStart(TaskHandler handler) {
        def msg = "{'type': 'process_start', 'name': '${handler.task.name}'}"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
    }

    @Override
    void onProcessComplete(TaskHandler handler) {
        def msg = "{'type': 'process_complete', 'name': '${handler.task.name}'}"
        broadcast.sendEvent( "message", msg.replaceAll("'",'"'));
    }

}
