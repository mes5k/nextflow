package nextflow.dag
import groovy.transform.CompileStatic
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class Vertex {

    // the operator instance
    def instance;

    Vertex( def operator ) {
        this.instance = operator
    }

}