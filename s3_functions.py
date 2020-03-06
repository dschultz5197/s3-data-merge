import boto3
from botocore.exceptions import ParamValidationError
import logging
from classes.s3_object import S3Object
import fnmatch
from classes.file_types import FileType
from classes.file_processing_data import FileProcessingData
from pandas.core.groupby.groupby import DataError
import pandas
import s3fs


def lookup_file(object_key):
    """
    Takes in the S3 Object Key and returns the file processing information.
    """
    if isinstance(object_key, str):
        # Populate list of known files. Currently hardcoded.
        # Future version will have an API set up to pull this data from dynamoDB table.
        file_types = [FileType("Email CSV", "user/userEmailFile*.csv", "mstr/userEmailFile.csv", "Email", ",", "\""),
                       FileType("Identifier CSV", "user/randomDataFile*.csv", "mstr/randomDataFile.csv", "Id", ",", "\"")]
        # Loop through known files to find the match to the currently posted file.
        for file_type in file_types:
            if fnmatch.fnmatch(object_key, file_type.incoming_file_pattern):
                # return the known file object once we have a match
                logging.info('File Type Found: {}'.format(file_type.file_process_name))
                return file_type
        # If there is no match, create new file type.
    return create_new_file_type(object_key)


def s3_file_exists(s3, s3_object):
    """
    Takes in the S3 Object Key and returns the file processing information.
    """
    try:
        bucket_obj = s3.Bucket(s3_object.bucket)
        objs = list(bucket_obj.objects.filter(Prefix=s3_object.key))
        if len(objs) > 0 and objs[0].key == s3_object.key:
            return True
    except TypeError as e:
        logging.info('Handling TypeError: {}'.format(e))
    except AttributeError as e:
        logging.info('Handling AttributeError: {}'.format(e))
    return False


def create_new_file_type(key):
    """
    Currently just returns a faked out a file type using the key passed in. Future versions where we use a data store for the
    file types will allow this function to write to the data store.
    """

    if not isinstance(key, str) or not key.startswith('user/'):
        logging.info('Invalid object key: {}'.format(key))
        return None
    file_type = FileType(file_process_name=key.replace('user/', ''),
                         incoming_file_pattern=key,
                         master_file_s3_key=key.replace('user/', 'mstr/'),
                         primary_key='Id',
                         field_delimiter=',',
                         text_qualifier='\"'
                         )
    return file_type


def get_dataframe(s3, s3_object, file_type):
    """
    Returns a data frame for the given object and type.
    If the object does not exist, returns an empty dataframe.
    """
    if not isinstance(file_type, FileType):
        logging.info('Bad file_type')
        return None
    try:
        if s3_file_exists(s3, s3_object):
            return pandas.read_csv(filepath_or_buffer=s3_object.path,
                                   delimiter=file_type.field_delimiter,
                                   quotechar=file_type.text_qualifier)
    except TypeError as e:
        logging.info('Handling TypeError: {}'.format(e))
    except AttributeError as e:
        logging.info('Handling AttributeError: {}'.format(e))
    except ParamValidationError as e:
        logging.info('Handling ParamValidationError: {}'.format(e))
    # Return an empty data frame containing the primary key.
    return pandas.DataFrame(columns=[file_type.primary_key])


def merge_to_mstr(user_object, file_type):
    """
    Reads the user and master objects into dataframes. Updates the master dataframe with changes from the user
    object and writes a new master object with these updates.
    """
    s3 = boto3.resource('s3')
    s3fs.S3FileSystem.cachable = False
    # set the master object
    mstr_object = S3Object(user_object.bucket, file_type.master_file_s3_key)
    # make sure the user object still exists
    if not s3_file_exists(s3, user_object):
        logging.info('User file does not exist.')
        return 'User file not found'

    if not user_object.key.startswith('user/'):
        logging.info('User object error')
        return 'User object error'

    try:
        logging.info('Loading stg dataframe.')
        # load the stg dataframe
        df_stg = get_dataframe(s3, user_object, file_type)
        logging.info('Loaded stg dataframe.')
        logging.info('Loading mstr dataframe.')
        # load the mstr dataframe
        df_mstr = get_dataframe(s3, mstr_object, file_type)
        logging.info('Loaded mstr dataframe.')
        # get an initial row count for the stg
        stg_initial_rowcount = len(df_stg.index)
        logging.info('stg_initial_count: {}'.format(stg_initial_rowcount))
        # dedupe the stg data on the identifier, just going to keep last since this is just a demo.
        df_stg.drop_duplicates(subset=[file_type.primary_key], keep='last', inplace=True)
        # count of records to update
        update_count = len(df_mstr.merge(df_stg, "inner", on=file_type.primary_key).index)
        logging.info('update_count: {}'.format(update_count))
        # create a concatenated dataframe
        logging.info('Loading new mstr dataframe.')
        df_mstr_new = pandas.concat([df_mstr, df_stg])
        df_mstr_new.drop_duplicates(subset=[file_type.primary_key], keep='last', inplace=True)
        logging.info('Loaded new mstr dataframe.')
        # dedupe, keeping last which will be from the stg
        # df_mstr_new.drop_duplicates(subset=[file_type.primary_key], keep='last', inplace=True)
        # set the processing stats
        mstr_prev_file_size = 0
        if s3_file_exists(s3, mstr_object):
            mstr_prev_file_size = s3.Object(mstr_object.bucket, mstr_object.key).content_length
        file_processing_data = FileProcessingData(stg_file_name=user_object.key.replace('user/', ''),
                                                  mstr_file_name=file_type.master_file_s3_key.replace('mstr/', ''),
                                                  stg_row_count=stg_initial_rowcount,
                                                  stg_column_count=len(df_stg.columns),
                                                  stg_file_size=s3.Object(user_object.bucket, user_object.key).content_length,
                                                  stg_duplicates=stg_initial_rowcount-len(df_stg.index),
                                                  stg_distinct_row_count=len(df_stg.index),
                                                  mstr_prev_row_count=len(df_mstr.index),
                                                  mstr_prev_column_count=len(df_mstr.columns),
                                                  mstr_prev_file_size=mstr_prev_file_size,
                                                  mstr_new_row_count=len(df_mstr_new.index),
                                                  mstr_new_column_count=len(df_mstr_new.columns),
                                                  update_count=update_count,
                                                  new_record_count=len(df_mstr.merge(df_stg, "right", on=file_type.primary_key).index) - update_count
                                                  )

        # write out the new file
        df_mstr_new.to_csv(mstr_object.path, index=False)
        file_processing_data.mstr_new_file_size = s3.Object(mstr_object.bucket, mstr_object.key).content_length
        logging.info(str(file_processing_data))
    except DataError as e:
        logging.info('Handling DataError: {}'.format(e))
        return 'Error'
    except KeyError as e:
        logging.info('Handling KeyError: {}'.format(e))
        return 'Key error'
    except Exception as e:
        logging.info('Handling Exception error: {}'.format(e))
        return 'Error'
    return 'Success'
