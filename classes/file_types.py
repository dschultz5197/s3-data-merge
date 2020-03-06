class FileType:
    def __init__(self, file_process_name, incoming_file_pattern, master_file_s3_key, primary_key, field_delimiter, text_qualifier):
        self.file_process_name = file_process_name
        self.incoming_file_pattern = incoming_file_pattern
        self.master_file_s3_key = master_file_s3_key
        self.primary_key = primary_key
        self.field_delimiter = field_delimiter
        self.text_qualifier = text_qualifier

    def __eq__(self, other):
        return self.file_process_name == other.file_process_name and self.incoming_file_pattern == other.incoming_file_pattern\
            and self.master_file_s3_key == other.master_file_s3_key and self.primary_key == other.primary_key
