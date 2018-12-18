# aws_code_pipeline_lambda
Lambda function to be used by CodePipeline that will combine the contents of two zip files into one.
This is a common requirement when you have two separate builds/repo for web UI code (for example React) and a separate build/repo with back-end code. If you want to deploy to Elastic Beanstalk then you need to combine both builds into a single deployable artifact.

This code can be expanded further to do other things while interacting with the pipeline.

For a NodeJS version you may want to look at https://github.com/Tanbouz/codepipeline-artifact-merge
