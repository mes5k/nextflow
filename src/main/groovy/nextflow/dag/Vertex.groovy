package nextflow.dag
import groovy.transform.CompileStatic
import groovy.transform.PackageScope

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class Vertex {

    private static int count = 0

    private int index

    // the operator instance
    def instance

    String label

    VertexType type

    Vertex( def operator, String label ) {
        this.instance = operator
        this.label = label
        this.index = count++
    }

    Vertex( VertexHandler handler ) {
        this.instance = handler.instance
        this.label = handler.instance
        this.type = handler.type
        this.index = count++
    }

    int getIndex() { index }

    @PackageScope
    static void reset() { count=0 }
}