package nextflow.dag

import groovy.transform.CompileStatic

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
interface GraphObserver {

    void onNewVertex( VertexHandler handler )

}
