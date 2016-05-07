package nextflow.dag

import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.script.InParam
import nextflow.script.InputsList
import nextflow.script.OutParam
import nextflow.script.OutputsList

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class VertexHandler {

    Object instance

    List<ChannelHandler> inbounds

    List<ChannelHandler> outbounds

    String label

    VertexType type

    protected VertexHandler() {

    }

    static VertexHandler newProcess(String label, def instance, InputsList inputs, OutputsList outputs) {
        new VertexHandler(
                instance: instance,
                inbounds: inputs.collect { InParam p -> new ChannelHandler(label: p.name, instance: p.inChannel) },
                outbounds: outputs.collect { OutParam p -> new ChannelHandler(label: p.name, instance: p.outChannel) },
                label: label,
                type: VertexType.PROCESS )
    }

    static VertexHandler newOperator(String label, def instance, def inputs, def outputs) {

        new VertexHandler(
                instance: instance,
                inbounds: normalize(inputs),
                outbounds: normalize(outputs),
                label: label,
                type: VertexType.OPERATOR )
    }

    static VertexHandler newOperator(String label, def instance, Map<String, Object> params ) {
        newOperator(label,instance, extractInputs(params), extractOutputs(params))
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
