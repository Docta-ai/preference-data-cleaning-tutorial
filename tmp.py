import requests
import json
from .utils import parse_json_response, check_status_code
import math
import requests
import hashlib
import os
from typing import List, Union
from tqdm import tqdm



class APIClient:
    def __init__(self, api_key, user_id):
        self.api_key = api_key
        self.base_url = 'https://api.docta.ai/api-key-'
        self.user_id = user_id

    def get_balance(self):
        headers = {
            'apiKey': self.api_key,
            'userId': self.user_id
        }
        response = requests.get(f'https://api.docta.ai/user/me', headers=headers)
        check_status_code(response)
        balance = parse_json_response(response)['usd']
        return f'Your Balance is: ${balance}'

    def get(self, model, params=None):
        headers = {
            'apiKey': self.api_key,
            'userId': self.user_id
        }
        payload = {
            "message": params
        }
        response = requests.get(f'{self.base_url + model}', headers=headers, params=payload)
        check_status_code(response)
        return parse_json_response(response)

    def post(self, model, data=None):
        headers = {
            'apiKey': self.api_key,
            'userId': self.user_id
        }
        payload = {
            "message": json.dumps(data)
        }
        response = requests.post(f'{self.base_url + model}', headers=headers, json=payload)
        check_status_code(response)
        return parse_json_response(response)

    def preference_data_cleaning(self, file_type: str, user_email: str, file_path: str, instruction_keys: Union[str, List[str]],
                             response_a: Union[str, List[str]], response_a_score: str = None, response_b: Union[str, List[str]] = None, 
                             response_b_score: str = None, return_score: bool = False, return_category: bool = False) -> None:
        '''
        file_type: the type of your file, csv or json
        user_email: your email to receive the result
        file_path: the path of your file on your local machine
        instruction_keys: the instruction keys, must be column name of csv
        response_a: the first response you want to compare, must be column name of csv
        response_a_score: the score of the first response, must be column name of csv
        response_b: the second response you want to compare, must be column name of csv
        response_b_score: the score of the second response, must be column name of csv
        return_score: if you want the score to be returned
        return_category: if you want the category to be returned
        '''
        hermes_url = 'https://api.docta.ai'

        instruction_keys = str(instruction_keys) if isinstance(instruction_keys, list) else instruction_keys
        response_a = str(response_a) if isinstance(response_a, list) else response_a
        response_b = str(response_b) if isinstance(response_b, list) else response_b

        def get_md5(file_bytes):
            md5 = hashlib.md5()
            md5.update(file_bytes)
            return md5.hexdigest()

        def get_base_file_name(file_path):
            return os.path.basename(file_path)

        def get_parent_parent_base(file_path):
            return os.path.basename(os.path.dirname(os.path.dirname(file_path)))

        file_name = get_base_file_name(file_path)
        folder_name = get_parent_parent_base(file_path)

        try:
            file_info = os.stat(file_path)
        except OSError as e:
            return

        chunk_size = 1024 * 1024 * 10  # 10MB
        num_chunks = math.ceil(file_info.st_size / chunk_size)

        try:
            with open(file_path, 'rb') as fi:
                file_bytes = fi.read()
        except IOError as e:
            return

        file_md5 = get_md5(file_bytes)
        client = requests.Session()
        headers = {
            'apiKey': self.api_key,
            'userId': self.user_id
        }

        for i in tqdm(range(int(num_chunks))):
            start = i * chunk_size
            end = min(start + chunk_size, file_info.st_size)
            chunk = file_bytes[start:end]
            chunk_md5 = get_md5(chunk)

            # 'folderType': 'text', 'fileType': 'text'
            fields = {
                'md5Value': file_md5,
                'originalFilename': file_name,
                'fileSize': str(len(chunk)),
                'folderType': 'text',
                'folderPath': '',
                'fileType': 'text',
                'chunk': str(i),
                'start': '0',
                'end': str(int(num_chunks) - 1),
                'chunks': str(int(num_chunks)),
                'chunkMd5': chunk_md5,
            }

            files = {
                'multipartFile': (file_name, chunk, 'application/octet-stream')
            }

            response = client.post(hermes_url + '/file/upload', headers=headers, data=fields, files=files)
            if response.status_code != 200:
                return parse_json_response(response)

        # 'folderType': 'text' 
        merge_params = {
            'md5Value': file_md5,
            'originalFilename': file_name,
            'folderType': 'text',
        }

        merge_response = client.post(hermes_url + '/file/merge', headers=headers, data=merge_params)
        if merge_response.status_code != 200:
            return parse_json_response(merge_response)
        
        time_est_val = int((2.3 * num_chunks + 13) // 60)
        time_est_clean = int(9 * num_chunks)
        print(f'File uploaded successfully. \nRunning data validation. Expected time: {time_est_val} to {time_est_val+1} minutes. \nPlease do not interrupt this process.\n------------------')


        params = {
            'originalFilename': file_name,
            'fileType': file_type,
            'instructionKeys': instruction_keys,
            'response_a': response_a,
            # 'response_a_score': response_a_score,
            # 'response_b': response_b,
            # 'response_b_score': response_b_score,
            'userEmail': user_email,
            "returnScore": return_score,
            "returnCategory": return_category
        }

        if response_a_score != None:
            params['response_a_score'] = response_a_score
        if response_b != None:
            params['response_b'] = response_b
        if response_b_score != None:
            params['response_b_score'] = response_b_score

        response = client.post(hermes_url + '/file/preference-data-cleaning', headers=headers, data=params)
        if response.status_code != 200:
            return parse_json_response(response)

        print(f'Validation passed. \nRunning data cleaning. Expected time: {time_est_clean} to {time_est_clean+10} minutes. \n------------------\nYou will receive an email with the download link once the process is completed. \nThis process may take some time, so please ensure you are not sending multiple requests simultaneously. \nIf you have any questions, please email us at contact@docta.ai.')
        return "success"
