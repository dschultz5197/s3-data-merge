import unittest
import app
from classes.file_types import FileType
import boto3
from classes.s3_object import S3Object
import pandas
from pandas.testing import assert_frame_equal
import s3_functions



class TestGetObject(unittest.TestCase):
    def test_get_object_from_message(self):
        """
        Test that when we send in a message as we'd receive from the SQS queue,
        we can get the s3 object from that message.
        """
        message_body = \
            {"Records":
                 [{"eventVersion": "2.1",
                   "eventSource": "aws:s3",
                   "awsRegion": "us-east-1",
                   "eventTime": "2020-02-22T21:28:03.647Z",
                   "eventName": "ObjectCreated:Put",
                   "userIdentity":
                       {"principalId": "AWS:AIDAQRSVXRI3PFEEFYUQT"},
                   "requestParameters":
                       {"sourceIPAddress": "162.154.145.78"},
                   "responseElements":
                       {"x-amz-request-id": "AD5EA4DE49B07804",
                        "x-amz-id-2": "oqQBThmYHD9w0uCW/4WLOTba2cgB8wG5wLwNRtB12coiyf9V5y0BcrJQPPlcHXv+xLlzgO8j1IOF6zIiZDNdFG5sx03PhVfM"},
                   "s3":
                       {"s3SchemaVersion": "1.0",
                        "configurationId": "s3-file-posted",
                        "bucket":
                            {"name": "some_bucket_name",
                             "ownerIdentity": {"principalId": "A2YX2TOSNCWXA9"},
                             "arn": "arn:aws:s3:::dschultz-python-skills-demo"},
                        "object":
                            {"key": "some_object_key",
                             "size": 883,
                             "eTag": "c6da448942001149be5b5728cbf7b422",
                             "sequencer": "005E519CE5135DFF6C"}
                        }
                   }
                  ]
             }
        result = app.get_object(message_body)
        self.assertEqual(result.path, 's3://some_bucket_name/some_object_key')

    def test_get_object_blank_message(self):
        # Test that when we send in a blank message we get back a blank object.
        message = {}
        result = app.get_object(message)
        self.assertIsNone(result)

    def test_get_object_invalid_message(self):
        # Test that when we send in an invalid message we get back a blank object
        message = {"Records": "This is some junk message format."}
        result = app.get_object(message)
        self.assertIsNone(result)

    def test_get_object_none(self):
        # Test that when we send in an invalid message we get back a blank object
        result = app.get_object(None)
        self.assertIsNone(result)


class TestLookupFile(unittest.TestCase):
    expected = FileType("Email CSV", "user/userEmailFile*.csv", "mstr/userEmailFile.csv", "Email", ",", "\"")
    expected_unknown = FileType("unit_test.csv", "user/unit_test.csv", "mstr/unit_test.csv", "Id", ",", "\"")

    def test_file_type_equals(self):
        test = FileType("Email CSV", "user/userEmailFile*.csv", "mstr/userEmailFile.csv", "Email", ",", "\"")
        result = (test == self.expected)
        self.assertTrue(result)

    def test_lookup_file_known_file(self):
        lookup = 'user/userEmailFile.csv'
        result = s3_functions.lookup_file(lookup)
        self.assertEqual(result, self.expected)

    def test_lookup_file_known_file_pattern(self):
        lookup = 'user/userEmailFile_PatternMatching.csv'
        result = s3_functions.lookup_file(lookup)
        self.assertEqual(result, self.expected)

    def test_lookup_file_bad_file_pattern(self):
        lookup = 'somejunkfile.csv'
        result = s3_functions.lookup_file(lookup)
        self.assertIsNone(result)

    def test_lookup_file_unknown_file_pattern_2(self):
        lookup = 'user/unit_test.csv'
        result = s3_functions.lookup_file(lookup)
        self.assertEqual(self.expected_unknown, result)

    def test_lookup_file_none(self):
        result = s3_functions.lookup_file(None)
        self.assertIsNone(result)

    def test_lookup_file_numeric(self):
        result = s3_functions.lookup_file(1234)
        self.assertIsNone(result)


class TestS3FileExists(unittest.TestCase):
    s3 = boto3.resource('s3')

    def test_s3_file_exists(self):
        test_object = S3Object('dschultz-python-skills-demo-unittests', 'objectexists.txt')
        result = s3_functions.s3_file_exists(self.s3, test_object)
        self.assertTrue(result)

    def test_s3_file_not_exist(self):
        test_object = S3Object('dschultz-python-skills-demo-unittests', 'objectnotexist.txt')
        result = s3_functions.s3_file_exists(self.s3, test_object)
        self.assertFalse(result)

    def test_s3_file_exists_none(self):
        result = s3_functions.s3_file_exists(None, None)
        self.assertFalse(result)

    def test_s3_file_exists_bad_object_type(self):
        result = s3_functions.s3_file_exists(self.s3, 'random junk string')
        self.assertFalse(result)

    def test_s3_file_exists_bad_object_type2(self):
        result = s3_functions.s3_file_exists(self.s3, 1234)
        self.assertFalse(result)

    def test_s3_file_exists_bad_s3_type(self):
        test_object = S3Object('dschultz-python-skills-demo-unittests', 'objectnotexist.txt')
        result = s3_functions.s3_file_exists('some junk', test_object)
        self.assertFalse(result)


class TestCreateNewFileType(unittest.TestCase):
    expected = FileType("unit_test.csv", "user/unit_test.csv", "mstr/unit_test.csv", "Id", ",", "\"")

    def test_create_new_file_tyep(self):
        result = s3_functions.create_new_file_type('user/unit_test.csv')
        self.assertEqual(self.expected, result)

    def test_create_new_file_type_bad_key(self):
        result = s3_functions.create_new_file_type('junk.csv')
        self.assertIsNone(result)

    def test_create_new_file_type_empty_key(self):
        result = s3_functions.create_new_file_type('')
        self.assertIsNone(result)

    def test_create_new_file_type_numeric(self):
        result = s3_functions.create_new_file_type(1234)
        self.assertIsNone(result)

    def test_create_new_file_type_none(self):
        result = s3_functions.create_new_file_type(None)
        self.assertIsNone(result)


class TestGetDataFrame(unittest.TestCase):
    s3 = boto3.resource('s3')

    def test_get_data_frame(self):
        d = {'Test': ['Test data'], 'Test2': ['More test data']}
        sample_df = pandas.DataFrame(data=d)
        sample_object = S3Object('dschultz-python-skills-demo-unittests', 'unit_test_sample.csv')
        file_type = FileType(file_process_name='unittest',
                             incoming_file_pattern='unittest',
                             master_file_s3_key='unittest',
                             primary_key='unittest',
                             field_delimiter=',',
                             text_qualifier='\"')
        result = s3_functions.get_dataframe(self.s3, sample_object, file_type)
        try:
            assert_frame_equal(sample_df, result)
            return True
        except AssertionError:
            return False

    def test_get_data_frame_bad_data(self):
        sample_df = pandas.DataFrame(columns=['unittest'])
        sample_object = S3Object('', 'unit_test_sample.csv')

        file_type = FileType(file_process_name='unittest',
                             incoming_file_pattern='unittest',
                             master_file_s3_key='unittest',
                             primary_key='unittest',
                             field_delimiter=',',
                             text_qualifier='\"')
        result = s3_functions.get_dataframe(self.s3, sample_object, file_type)
        try:
            assert_frame_equal(sample_df, result)
            return True
        except AssertionError:
            return False

    def test_get_data_frame_bad_data2(self):
        sample_df = pandas.DataFrame(columns=['unittest'])
        sample_object = S3Object('', 'unit_test_sample.csv')

        file_type = FileType(file_process_name='unittest',
                             incoming_file_pattern='unittest',
                             master_file_s3_key='unittest',
                             primary_key='unittest',
                             field_delimiter=',',
                             text_qualifier='\"')
        result = s3_functions.get_dataframe(self.s3, sample_object, file_type)
        try:
            assert_frame_equal(sample_df, result)
            return True
        except AssertionError:
            return False

    def test_get_data_frame_bad_data3(self):
        sample_df = pandas.DataFrame(columns=['unittest'])
        sample_object = S3Object('', 1234)

        file_type = FileType(file_process_name='unittest',
                             incoming_file_pattern='unittest',
                             master_file_s3_key='unittest',
                             primary_key='unittest',
                             field_delimiter=',',
                             text_qualifier='\"')
        result = s3_functions.get_dataframe(self.s3, sample_object, file_type)
        try:
            assert_frame_equal(sample_df, result)
            return True
        except AssertionError:
            return False

    def test_get_data_frame_bad_data4(self):
        sample_df = pandas.DataFrame()
        sample_object = S3Object('', 'unit_test_sample.csv')

        result = s3_functions.get_dataframe(None, sample_object, None)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
