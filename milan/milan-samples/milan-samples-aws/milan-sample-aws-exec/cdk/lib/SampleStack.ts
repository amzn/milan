import {Duration, Stack, StackProps} from "aws-cdk-lib";
import {Construct} from "constructs";
import {AttributeType, BillingMode, StreamViewType, Table} from "aws-cdk-lib/aws-dynamodb";
import {constructStatelessSampleLambdaFunction} from "./generated/StatelessSample";


export class SampleStack extends Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const inputTable = new Table(this, "InputTable", {
            tableName: "StatelessSampleInputTable",
            partitionKey: {name: "recordId", type: AttributeType.STRING},
            billingMode: BillingMode.PAY_PER_REQUEST,
            stream: StreamViewType.NEW_IMAGE,
        });

        const outputTable = new Table(this, "OutputTable", {
            tableName: "StatelessSampleOutputTable",
            partitionKey: {name: "recordId", type: AttributeType.STRING},
            billingMode: BillingMode.PAY_PER_REQUEST,
        });

        const statelessSampleLambda = constructStatelessSampleLambdaFunction(this, {
            inputInputTable: inputTable,
            outputOutputTable: outputTable,
            timeout: Duration.seconds(30),
            memorySize: 256,
        });
    }
}
