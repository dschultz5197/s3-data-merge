import boto3
from datetime import datetime, timedelta
import json
import logging
import configparser
from classes.s3_object import S3Object
import s3_functions
from botocore.exceptions import ClientError


def get_object(message_body):
    """
    Takes in the message body as a json object and returns the S3Object parsed from the message body.
    """
    try:
        parsed_s3_object = S3Object(message_body['Records'][0]['s3']['bucket']['name'], message_body['Records'][0]['s3']['object']['key'])
    except KeyError as e:
        # log the error
        logging.info('Handling KeyError: ' + str(e))
        parsed_s3_object = None
    except TypeError as e:
        # log the error
        logging.info('Handling TypeError' + str(e))
        parsed_s3_object = None
    except AttributeError as e:
        # log the error
        logging.info('Handling AttributeError' + str(e))
        parsed_s3_object = None
    return parsed_s3_object


def send_sqs_message(ec2_instance_id, outgoing_message_queue_name):
    # set the sqs resource
    sqs = boto3.resource('sqs')
    logging.info('Shutting down EC2')
    queue_shutdown = sqs.get_queue_by_name(QueueName=outgoing_message_queue_name)
    msg_data = {}
    msg_data['msgBody'] = "Stop Instance"
    msg_data['msgAttributes'] = {'instance_id': {'StringValue': ec2_instance_id, 'DataType': 'String'},
                                'action': {'StringValue': 'stop', 'DataType': 'String'}}
    try:
        response = queue_shutdown.send_message(MessageBody=msg_data['msgBody'], MessageAttributes=msg_data['msgAttributes'])
        logging.info(response)
    except ClientError as e:
        logging.error(e)
    return True


def main():
    config = configparser.ConfigParser()
    config.read('skills-demo.config')

    ec2_instance_id = config['default']['ec2_instance_id']
    minutes_without_message_limit = config['default']['minutes_without_message_limit']
    incoming_message_queue_name = config['default']['incoming_message_queue_name']
    outgoing_message_queue_name = config['default']['outgoing_message_queue_name']

    logging.basicConfig(level=logging.INFO, filemode='w', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
    logging.info('Application Starting')
    logging.info('EC2 Instance: ' + ec2_instance_id)
    logging.info('Incoming message queue: ' + incoming_message_queue_name)
    logging.info('Outgoing message queue: ' + outgoing_message_queue_name)
    # set last message received to current time.
    last_message_received = datetime.now()
    # set the sqs resource
    sqs = boto3.resource('sqs')
    # get the incoming queue
    queue = sqs.get_queue_by_name(QueueName=incoming_message_queue_name)

    while last_message_received > datetime.now() - timedelta(minutes=int(minutes_without_message_limit)):
        # poll the queue
        logging.info('Polling Queue: {}'.format(incoming_message_queue_name))
        messages = queue.receive_messages(WaitTimeSeconds=20)
        # if message received, and it's not empty.
        if bool(messages):
            logging.info('Message received')
            # reset last_message_received
            last_message_received = datetime.now()
            logging.info('Received Message: {}'.format(last_message_received))
            # loop through the messages received
            for message in messages:
                # process the message body to get the S3Object
                user_object = get_object(json.loads(message.body))
                result = ""
                logging.info('User Object: {}'.format(user_object.path))
                # make sure the object could be parsed from the message
                if bool(user_object):
                    logging.info('Processing the file: {}'.format(user_object.path))
                    # look for known file matching the pattern of the current message
                    file_type = s3_functions.lookup_file(user_object.key)
                    # do the upsert...
                    if bool(file_type):
                        result = s3_functions.merge_to_mstr(user_object, file_type)
                # if we were unsuccessful in parsing the message log it.
                else:
                    logging.error('Invalid message format received.')
                # Right now we're just going to remove all processed messages
                # whether they are valid or invalid.
                # Later version will set up a dead letter queue to allow for checking bad messages.
                logging.info('Merge Result: {}'.format(result))
                message.delete()
                logging.info('Message removed from queue.')
    send_sqs_message(ec2_instance_id, outgoing_message_queue_name)
    logging.info('Completed')


if __name__ == '__main__':
    main()
