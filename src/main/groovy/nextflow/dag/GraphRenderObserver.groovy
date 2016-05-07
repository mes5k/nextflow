package nextflow.dag
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.trace.TraceObserver

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class GraphRenderObserver implements TraceObserver, GraphObserver {

    static final String DEF_FILE_NAME = 'dag.dot'

    private Path dotFile

    private Path pngFile

    private DAG dag

    GraphRenderObserver( Path file ) {
        this.dotFile = file
        this.pngFile = file.resolveSibling( "${file.baseName}.png" )
    }

    @Override
    void onFlowStart(Session session) {
        this.dag = session.dag
    }

    @Override
    void onFlowComplete() {
        dotFile.text = dag.toString()
        // run graphviz to render the graph
        def cmd = "command -v dot &>/dev/null  && dot -Tpng ${dotFile} > ${pngFile}"
        log.debug "Render graph cmd: `cmd`"
        ["bash","-c", cmd].execute()

    }


    @Override
    void onNewVertex(VertexHandler handler) {
        dag.addVertex(handler)
    }

    @Override
    void onProcessCreate(TaskProcessor process) {

    }


    @Override
    void onProcessSubmit(TaskHandler handler) {

    }

    @Override
    void onProcessStart(TaskHandler handler) {

    }

    @Override
    void onProcessComplete(TaskHandler handler) {

    }

}
