package nextflow.dag
import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.script.InParam
import nextflow.script.InputsList
import nextflow.script.OutParam
import nextflow.script.OutputsList
import nextflow.script.SetInParam
import nextflow.script.SetOutParam

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GraphEvent {

    String label

    List<ChannelHandler> inbounds

    List<ChannelHandler> outbounds

    DAG.VertexType type

    protected GraphEvent() {

    }

    static GraphEvent newProcess(String label, InputsList inputs, OutputsList outputs) {

        new GraphEvent(
                label: label,
                inbounds: normalizeInputs(inputs),
                outbounds: normalizeOutputs(outputs),
                type: DAG.VertexType.PROCESS )
    }

    static GraphEvent newOperator(String label, def inputs, def outputs) {

        new GraphEvent(
                label: label,
                inbounds: normalize(inputs),
                outbounds: normalize(outputs),
                type: DAG.VertexType.OPERATOR )
    }

    static GraphEvent newOperator(String label, Map<String, Object> params ) {
        newOperator(label, extractInputs(params), extractOutputs(params))
    }


    static private List<ChannelHandler> normalizeInputs( InputsList inputs ) {
        inputs.collect { InParam p ->
            new ChannelHandler(instance: p.inChannel, label: p instanceof SetInParam ? null : p.name)
        }
    }

    static private List<ChannelHandler> normalizeOutputs( OutputsList outputs ) {
        outputs.collect { OutParam p ->
            new ChannelHandler(instance: p.outChannel, label: p instanceof SetOutParam ? null : p.name)
        }
    }

    static private List<ChannelHandler> normalize( entry ) {
        if( entry == null ) {
            Collections.emptyList()
        }
        else if( entry instanceof Collection ) {
            entry.collect { new ChannelHandler(instance: it) }
        }
        else {
            [ new ChannelHandler(instance: entry) ]
        }
    }

    static List<DataflowChannel<?>> extractInputs(final Map<String, Object> params) {
        final List<DataflowChannel<?>> inputs = (List<DataflowChannel<?>>) params.get(DataflowProcessor.INPUTS);
        if (inputs == null) return Collections.emptyList();
        return Collections.unmodifiableList(inputs);
    }

    static List<DataflowChannel<?>> extractOutputs(final Map<String, Object> params) {
        final List<DataflowChannel<?>> outputs = (List<DataflowChannel<?>>) params.get(DataflowProcessor.OUTPUTS);
        if (outputs == null) return Collections.emptyList();
        return Collections.unmodifiableList(outputs);
    }

}
