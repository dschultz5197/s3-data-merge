# s3-data-merge
This process is a sample application showing AWS integrations.


The flow of this will be:
  User posts a file to a specified S3 location
  S3 sends message to SQS queue
  Application will be running on an EC2 instance and polls SQS queue.
  Application will compare the user file with a master file previously processed.
  The file must include expected primary key.
  Records which exist in both will be updated, new records will be added.
  Stats will be logged.
  After no messages for a given time period the application will send an SQS message and stop.
  The SQS message will be processed by a Lambda function which will stop the EC2 instance. 
  This is done to avoid having to pay for an EC2 instance while no one is interacting with the application.

