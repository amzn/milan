# AWS Lambda Sample
This sample shows how to build and deploy a Milan application to an AWS Lambda function.

Like other samples, it has two sub-projects:
* `milan-sample-aws-gen` contains the Milan application definition.
* `milan-sample-aws-exec` performs the Milan code generation which generates a scala file and a typescript file containing the AWS CDK contructs needed to deploy the application to AWS.
The `pom.xml` file in this project is where the correct Milan compiler is invoked.

## How to use

1. First make sure that you have the Milan packages installed in your local maven repo. 
   1. The fastest way to do this, is from the root of the Milan source tree, run `mvn install -Dnoflink -DskipTests`.
1. Next, run the maven build for this sample.
   1. From this folder (`/milan-samples/milan-samples-aws/`) run `mvn package`.
   1. This generates the scala files for the application in the `milan-samples-aws-exec` project and compiles them into a jar that includes a lambda handler that executes the application logic.
   1. It also generates a typescript file in `milan-samples-aws-exec/cdk/lib/generated` that can create the lambda function construct to host the application.
    The method exported by this typescript file must be called from the user's CDK stack, as shown in `cdk/lib/SampleStack.ts`.
1. Finally, deploy the generated application.
   1. See the [CDK documentation](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html) for how to bootstrap your AWS account for CDK deployment.
   1. From the `milan-samples-aws-exec/cdk` folder under this one, run `cdk deploy`.
