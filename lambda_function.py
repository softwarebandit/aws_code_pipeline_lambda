import boto3
import botocore
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
import zipfile

import sys

s3 = boto3.resource('s3')
codepipeline = boto3.client('codepipeline')

def download(bucket_name, object_key):
    # The filename generation should be more unique in case both files have same name at different paths.
    # ... it's ok for now
    file_name = '/tmp/' + object_key.split('/')[-1]
    print("Downloading " + bucket_name + "/" + object_key + " to " + file_name)
    try:
        s3.Bucket(bucket_name).download_file(object_key, file_name)
        return file_name
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print('Object not found: ' + object_key)
        else:
            raise

def upload(bucket_name, object_key, file_name):
    print("Uploading " + file_name + " to " + bucket_name + "/" + object_key)
    # GB = 1024 ** 3
    # config = TransferConfig(multipart_threshold=5 * GB)
    #s3.Bucket(bucket_name).upload_file(file_name, object_key, Config = TransferConfig(multipart_threshold=5 * GB))
    s3.meta.client.upload_file(file_name,
                               bucket_name,
                               object_key,
                               ExtraArgs={'ServerSideEncryption':'aws:kms', 'SSEKMSKeyId':'alias/aws/s3', 'ContentType':'application/zip'})

def zip_merge(file1, file2, file1_path_for_file2):
    if not file1_path_for_file2.endswith('/'):
        file1_path_for_file2 = file1_path_for_file2 + '/'
    zfile1 = zipfile.ZipFile(file1 , 'a')
    zfile2 = zipfile.ZipFile(file2, 'r')
    for filename in zfile2.namelist():
        merged_filename = file1_path_for_file2 + filename
        if merged_filename not in zfile1.namelist():
            print("Merging " + merged_filename)
            zfile1.writestr(merged_filename, zfile2.open(filename).read())
        else:
            print("Skpping " + merged_filename)
    zfile1.close()
    print("Completed merging")

def combine(bucket_name1, object_key1, bucket_name2, object_key2, out_bucket_name, out_object_key, merge_path):
    file1 = download(bucket_name1, object_key1)
    file2 = download(bucket_name2, object_key2)
    zip_merge(file1, file2, merge_path)
    upload(out_bucket_name, out_object_key, file1)
    print("Combine completed")

def notify_codepipeline_success(job_id, message):
    print('CodePipeline Job Succeeded:' + job_id + ' ' + message)
    codepipeline.put_job_success_result(jobId=job_id)

def notify_codepipeline_failure(job_id, message):
    print('CodePipeline Job Failed:' + job_id + ' ' + message)
    codepipeline.put_job_failure_result(jobId=job_id, failureDetails={'message': message, 'type': 'JobFailed'})

def lambda_handler(event, context):
    global s3
    try:
        print event
        if not event.has_key('CodePipeline.job'):
            return 'Not a CodePipeline job. Exiting...'

        job_id = event['CodePipeline.job']['id']

        input_artifacts = event['CodePipeline.job']['data']['inputArtifacts']

        artifact1 = input_artifacts[0]
        if not artifact1.has_key('location') or artifact1['location']['type']!='S3':
            message = 'Artifact1 location type is expected to be S3'
            notify_codepipeline_failure(job_id, message)
            return message

        artifact2 = input_artifacts[1]
        if not artifact2.has_key('location') or artifact2['location']['type']!='S3':
            message = 'Artifact2 location type is expected to be S3'
            notify_codepipeline_failure(job_id, message)
            return message

        output_artifacts = event['CodePipeline.job']['data']['outputArtifacts']
        if len(output_artifacts) != 1:
            message = 'One output artifacts is required'
            notify_codepipeline_failure(job_id, message)
            return message

        output_artifact = output_artifacts[0]
        if not output_artifact.has_key('location') or output_artifact['location']['type']!='S3':
            message = 'Output artifact location type is expected to be S3'
            notify_codepipeline_failure(job_id, message)
            return message

        insert_path = event['CodePipeline.job']['data']['actionConfiguration']['configuration']['UserParameters']
        print("insert_path:" + insert_path)

        key_id = event['CodePipeline.job']['data']['artifactCredentials']['accessKeyId']
        key_secret = event['CodePipeline.job']['data']['artifactCredentials']['secretAccessKey']
        session_token = event['CodePipeline.job']['data']['artifactCredentials']['sessionToken']
        s3 = boto3.resource('s3',
                            aws_access_key_id=key_id,
                            aws_secret_access_key=key_secret,
                            aws_session_token=session_token)

        combine(artifact1['location']['s3Location']['bucketName'],
                artifact1['location']['s3Location']['objectKey'],
                artifact2['location']['s3Location']['bucketName'],
                artifact2['location']['s3Location']['objectKey'],
                output_artifact['location']['s3Location']['bucketName'],
                output_artifact['location']['s3Location']['objectKey'],
                'public')

        notify_codepipeline_success(job_id, 'done')
        return 'done'
    except:
        if 'job_id' in vars() or 'job_id' in globals():
            notify_codepipeline_failure(job_id, str(sys.exc_info()))
        return "Unexpected Error"




