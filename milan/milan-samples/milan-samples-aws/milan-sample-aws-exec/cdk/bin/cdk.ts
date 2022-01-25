import {App} from "aws-cdk-lib";
import {SampleStack} from "../lib/SampleStack";

const app = new App();

new SampleStack(app, "SampleStack");
