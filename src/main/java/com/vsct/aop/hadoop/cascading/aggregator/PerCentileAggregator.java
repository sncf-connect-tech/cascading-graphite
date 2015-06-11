package com.vsct.aop.hadoop.cascading.aggregator;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import java.util.ArrayList;

public class PerCentileAggregator extends BaseOperation<PerCentileAggregator.Context> implements Aggregator<PerCentileAggregator.Context> {


    private int centile;

    public PerCentileAggregator(Fields fieldDeclaration, int centile) {
        super(1, fieldDeclaration);
        this.centile = centile;
        if (!fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1)
            throw new IllegalArgumentException("fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size());
    }

    @Override
    public void start(FlowProcess flowProcess,
                      AggregatorCall<Context> aggregatorCall) {
        aggregatorCall.getContext().reset();

    }

    @Override
    public void aggregate(FlowProcess flowProcess,
                          AggregatorCall<Context> aggregatorCall) {
        Context context = aggregatorCall.getContext();
        TupleEntry arguments = aggregatorCall.getArguments();

        context.values.add(arguments.getDouble(0));

    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        operationCall.setContext(new Context());
    }

    @Override
    public void complete(FlowProcess flowProcess,
                         AggregatorCall<Context> aggregatorCall) {
        aggregatorCall.getOutputCollector().add(getResult(aggregatorCall));

    }

    private Tuple getResult(AggregatorCall<Context> aggregatorCall) {
        return aggregatorCall.getContext().result();
    }

    protected class Context {
        Tuple tuple = Tuple.size(1);
        ArrayList<Double> values;


        public Context reset() {
            values = new ArrayList<Double>();
            return this;
        }

        public Tuple result() {
            double[] doubleValues = new double[values.size()];
            Percentile percentile = new Percentile(PerCentileAggregator.this.centile);
            for (int i = 0; i < values.size(); i++) doubleValues[i] = values.get(i);
            float fract = (float) percentile.evaluate(doubleValues);
            tuple.set(0, fract);
            return tuple;
        }
    }

}
