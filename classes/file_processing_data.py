class FileProcessingData:

    def __init__(self,
                 stg_file_name,
                 mstr_file_name,
                 stg_row_count=0,
                 stg_column_count=0,
                 stg_file_size=0,
                 stg_duplicates=0,
                 stg_distinct_row_count=0,
                 mstr_prev_row_count=0,
                 mstr_prev_column_count=0,
                 mstr_prev_file_size=0,
                 mstr_new_row_count=0,
                 mstr_new_column_count=0,
                 mstr_new_file_size=0,
                 update_count=0,
                 new_record_count=0
                 ):
        self.stg_file_name = stg_file_name
        self.mstr_file_name = mstr_file_name
        self.stg_row_count = stg_row_count
        self.stg_column_count = stg_column_count
        self.stg_file_size = stg_file_size
        self.stg_duplicates = stg_duplicates
        self.stg_distinct_row_count = stg_distinct_row_count
        self.mstr_prev_row_count = mstr_prev_row_count
        self.mstr_prev_column_count = mstr_prev_column_count
        self.mstr_prev_file_size = mstr_prev_file_size
        self.mstr_new_row_count = mstr_new_row_count
        self.mstr_new_column_count = mstr_new_column_count
        self.mstr_new_file_size = mstr_new_file_size
        self.update_count = update_count
        self.new_record_count = new_record_count

    def __str__(self):
        data_string = 'stg_file_name: {} \n'.format(self.stg_file_name)
        data_string = data_string + 'mstr_file_name: {}\n'.format(self.mstr_file_name)
        data_string = data_string + 'stg_row_count: {}\n'.format(self.stg_row_count)
        data_string = data_string + 'stg_column_count: {}\n'.format(self.stg_column_count)
        data_string = data_string + 'stg_file_size: {}\n'.format(self.stg_file_size)
        data_string = data_string + 'stg_duplicates: {}\n'.format(self.stg_duplicates)
        data_string = data_string + 'stg_distinct_row_count: {}\n'.format(self.stg_distinct_row_count)
        data_string = data_string + 'mstr_prev_row_count: {}\n'.format(self.mstr_prev_row_count)
        data_string = data_string + 'mstr_prev_column_count: {}\n'.format(self.mstr_prev_column_count)
        data_string = data_string + 'mstr_prev_file_size: {}\n'.format(self.mstr_prev_file_size)
        data_string = data_string + 'mstr_new_row_count: {}\n'.format(self.mstr_new_row_count)
        data_string = data_string + 'mstr_new_column_count: {}\n'.format(self.mstr_new_column_count)
        data_string = data_string + 'mstr_new_file_size: {}\n'.format(self.mstr_new_file_size)
        data_string = data_string + 'update_count: {}\n'.format(self.update_count)
        data_string = data_string + 'new_record_count: {}\n'.format(self.new_record_count)
        return data_string

