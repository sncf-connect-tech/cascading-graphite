package com.vsct.aop.hadoop.cascading.aggregator;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.HashSet;
import java.util.Set;

public class ErrorRateAggregator extends BaseOperation<ErrorRateAggregator.Context> implements Aggregator<ErrorRateAggregator.Context> {

    private boolean countByStatusCodeOK;
    private Set<String> statusCodeSet = new HashSet<>();
    
    protected class Context {
        Tuple tuple = Tuple.size( 1 );
        int nbStatusCodeOK = 0;
        int nbStatusCodeKO = 0;


        public Context reset()
        {
            this.nbStatusCodeOK = 0;
            this.nbStatusCodeKO = 0;
            return this;
        }

        public void addStatusCode(String code){
            if(countByStatusCodeOK) {
                if (statusCodeSet.contains(code)) {
                    nbStatusCodeOK++;
                } else {
                    nbStatusCodeKO++;
                }
            } else {
                if (!statusCodeSet.contains(code)) {
                    nbStatusCodeOK++;
                } else {
                    nbStatusCodeKO++;
                }
            }
        }

        public Tuple result()
        {
            int sumCodes = nbStatusCodeKO + nbStatusCodeOK;
            Float ratio = 0.0f;
            if(sumCodes != 0){
                ratio = ((((float) nbStatusCodeKO)/sumCodes)*100);
            }
            tuple.set(0, ratio);
            return tuple;
        }
    }

    public ErrorRateAggregator(Fields fieldDeclaration, Set<String> statusCodeSet, boolean countByStatusCodeOK){
        super( 1, fieldDeclaration);

        if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
            throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );

        this.statusCodeSet = statusCodeSet;
        this.countByStatusCodeOK = countByStatusCodeOK;
    }

    /**
     * By default the Http status ok is used (status code 200)
     * @param fieldDeclaration name of the new column containing the status
     */
    public ErrorRateAggregator(Fields fieldDeclaration){
        this(fieldDeclaration, new HashSet<String>(), true);
        //Default Http code ok
        statusCodeSet.add("200");
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

        context.addStatusCode(arguments.getString(0));
    }

    @Override
    public void complete(FlowProcess flowProcess,
                         AggregatorCall<Context> aggregatorCall) {
        aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
        operationCall.setContext( new Context() );
    }

    private Tuple getResult(AggregatorCall<Context> aggregatorCall )
    {
        return aggregatorCall.getContext().result();
    }
}
