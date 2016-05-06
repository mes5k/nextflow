package nextflow.trace
import java.nio.file.Path

import nextflow.Session
import nextflow.dag.DAG
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class DagObserver implements TraceObserver {

    static final String DEF_FILE_NAME = 'dag.dot'

    private Path file

    private DAG dag

    DagObserver( Path file ) {
        this.file = file
    }

    @Override
    void onFlowStart(Session session) {
        this.dag = session.dag
    }

    @Override
    void onFlowComplete() {
        file.text = dag.toString()
    }

    @Override
    void onFlowError(Throwable error) {

    }

    @Override
    void onProcessCreate(TaskProcessor process) {

    }

    @Override
    void onProcessDestroy(TaskProcessor process) {

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

    @Override
    void onProcessError(TaskHandler handler, Throwable error) {

    }
}
