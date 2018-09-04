package com.bxdemo.redshift.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.bxdemo.redshift.lambda.Redshiftconnect;
public class LambdaFunctionHandler implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);

        // TODO: implement your handler
        Redshiftconnect rc = new Redshiftconnect();
        String result = rc.calls();
        return result;
    }

}
