import yaml
import boto3
import sys
import functools
import telebot
import os
import json
import logging
from io import BytesIO
import requests
import time
import sqlite3
import __main__

with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yml'), 'r') as config_file:
    config = yaml.load(config_file, yaml.FullLoader)

# YANDEX API
YANDEX_API_ID_KEY = config['credentials']['yandex_api']['YANDEX_API_ID_KEY']
YANDEX_API_SECRET_KEY = config['credentials']['yandex_api']['YANDEX_API_SECRET_KEY']
API_AUTH_HEADER = {'Authorization': 'Api-Key {}'.format(YANDEX_API_SECRET_KEY)}

# YANDEX_S3
YANDEX_STATIC_ID_KEY = config['credentials']['yandex_static']['YANDEX_STATIC_ID_KEY']
YANDEX_STATIC_SECRET_KEY = config['credentials']['yandex_static']['YANDEX_STATIC_SECRET_KEY']
REGION_NAME = config['credentials']['yandex_static']['REGION_NAME']
OBJECT_STORAGE_API_LINK = config['credentials']['yandex_static']['OBJECT_STORAGE_API_LINK']
BUCKET_NAME = config['credentials']['yandex_static']['BUCKET_NAME'].replace('_', '-')

yandex_storage_client = boto3.client(
    's3',
    aws_access_key_id=YANDEX_STATIC_ID_KEY,
    aws_secret_access_key=YANDEX_STATIC_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=OBJECT_STORAGE_API_LINK
)

# BOT
BOT_KEY = config['credentials']['bot']['bot_key']
MAX_DURATION = config['bot_config']['max_duration']
bot = telebot.TeleBot(BOT_KEY)

# DB
try:
    DB_NAME = config['database']['name']
except KeyError:
    logging.warning('Update your config.yml add - database name as in config.yml.example, using default name')
    DB_NAME = 'listener_bot_db'
DB_NAME = os.path.join(os.path.dirname(__main__.__file__), DB_NAME)

# INITIALIZATIONS


def init_object_storage(client, bucket_name):
    logging.info('Initializing Yandex Object Storage')

    buckets_list = client.list_buckets()
    buckets_list = [bucket['Name'] for bucket in buckets_list['Buckets']]

    logging.debug('List of buckets: {}'.format(', '.join(buckets_list)))

    if bucket_name in buckets_list:
        logging.debug('Bucket already exists, passing')
        logging.info('Yandex Object Storage check passed')
    else:
        logging.info(f'Bucket does not exists, creating one with a name \'{bucket_name}\'')
        try:
            create_bucket_response = client.create_bucket(Bucket=bucket_name)

            if create_bucket_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logging.info('Bucket successfully created')

        except Exception as e:
            logging.error(f'Something is wrong, probably with bucket name try different one, error text:\n{e}')
            sys.exit(1)


check_buckets = functools.partial(init_object_storage, yandex_storage_client, BUCKET_NAME)


def init_users():
    logging.info('Checking admin id in allowed users')
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'allowed.json'), 'r') as allowed_file:
        allowed = json.load(allowed_file)
        admin_id = config['credentials']['bot']['admin_id']

        if admin_id not in allowed['allowed_users']:
            allowed['allowed_users'].append(admin_id)
            logging.info('Admin ID not in allowed users list, adding')

            with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'allowed.json'), 'w') as allowed_file:
                json.dump(allowed, allowed_file)

            logging.info('Admin ID added')
            return

    logging.info('Admin ID check passed')


def init_database():
    db_connection = sqlite3.connect(f'{DB_NAME}.db')
    cursor = db_connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS records_transcribations ([record_id] text, [transcribation] text)')
    db_connection.close()

    logging.info('Database check passed')


# BOT STUFF
def get_allowed_users_and_channels():
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'allowed.json'), 'r') as allowed_file:
        allowed = json.load(allowed_file)

    return allowed['allowed_users'], allowed['allowed_channels']


def add_channels(chat_id):
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'allowed.json'), 'r') as allowed_file:
        allowed = json.load(allowed_file)

    if chat_id in allowed['allowed_channels']:
        return 'Channel already in allowed channels list'
    elif chat_id not in allowed['allowed_channels']:
        allowed['allowed_channels'].append(chat_id)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'allowed.json'), 'w') as allowed_file:
            json.dump(allowed, allowed_file)
        return 'Channel added to list'


def allowed_presence_check(allowed_users, chat_id):
    for user in allowed_users:
        try:
            chat_user = bot.get_chat_member(chat_id, user)
            logging.debug(f'User {user} was found in chat {chat_id}')
            if chat_user.status == 'left':
                return False
            return True
        except telebot.apihelper.ApiException as e:
            logging.debug(f'User {user} wasn\'t found in chat {chat_id}')

    logging.info(f'Bot was tried to be used in a chat {chat_id} where no allowed user was in')
    return False


def send_greeting(chat_id):
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sounds/greetings.oga'), 'rb') as greeting:
        bot.send_voice(chat_id, greeting)


class VoiceMessage:

    def __init__(self, message):
        self.message = message
        self.duration = message.voice.duration
        self.file_id = message.voice.file_id
        self.file_url = bot.get_file_url(self.file_id)
        self.transcribed_text = None
        self.voice_id = message.json['voice']['file_unique_id']

    def get_file(self):
        voice_msg_request = requests.get(self.file_url)
        voice_msg_object = BytesIO(voice_msg_request.content)

        return voice_msg_object

    def upload_to_object_storage(self):
        voice_msg_object = self.get_file()
        logging.debug(f'Upload method initialize, message url - {self.file_url}')
        yandex_storage_client.upload_fileobj(voice_msg_object,
                                             BUCKET_NAME,
                                             f'{self.file_id}.oga')

        logging.debug(f'File {self.file_id} uploaded to Yandex Object Storage')

    def delete_from_object_storage(self):
        yandex_storage_client.delete_object(Bucket=BUCKET_NAME, Key=f'{self.file_id}.oga')

        logging.debug(f'File {self.file_id} deleted from Yandex Object Storage')

    def transcribe_short(self):
        file = self.get_file()

        transcribation_request = requests.post('https://stt.api.cloud.yandex.net/speech/v1/stt:recognize',
                                               data=file.read(),
                                               headers=API_AUTH_HEADER
                                               )

        if transcribation_request.status_code == 200:
            msg_text = json.loads(transcribation_request.content)
            self.transcribed_text = msg_text['result']

            logging.info(
                f'Just transcribed SHORT voice message in chat:{self.message.chat.id} from user:'
                + f'{self.message.from_user.id} ({self.message.from_user.username}), '
                + f'message text: \'{self.transcribed_text}\''
            )

        else:
            req_status_code = transcribation_request.status_code
            req_status_content = json.loads(transcribation_request.content)

            logging.warning(
                f'Transcribation of short message failed:\nCode:{req_status_code}\nContent:{req_status_content}')

    def join_long_text(self, chunks):

        long_text = ''

        for chunk in chunks:
            long_text = long_text + chunk['alternatives'][0]['text'] + ' '

        self.transcribed_text = long_text.strip()

        logging.info(
            f'Just transcribed LONG voice message in chat:{self.message.chat.id} from user:{self.message.from_user.id}'
            + f' ({self.message.from_user.username}), message text: \'{self.transcribed_text}\''
        )

    def transcribe_long(self):

        self.upload_to_object_storage()

        voice_msg_temp_link = os.path.join(OBJECT_STORAGE_API_LINK, BUCKET_NAME, f'{self.file_id}.oga')

        req_params = {
            "config": {
                "specification": {
                    "languageCode": "ru-RU",
                    "profanityFilter": "false",
                    "audioEncoding": "OGG_OPUS"
                }
            },
            "audio": {
                "uri": voice_msg_temp_link
            }
        }

        transcribation_request = requests.post(
            'https://transcribe.api.cloud.yandex.net/speech/stt/v2/longRunningRecognize',
            json=req_params,
            headers=API_AUTH_HEADER
        )

        trans_req_json = json.loads(transcribation_request.content)
        logging.debug(
            f'Uploaded and send transcribation request for long voice message, ' +
            f'status:{transcribation_request.status_code}, id:{trans_req_json["id"]}')

        retries = 0
        retries_amount = 15
        while retries < retries_amount:

            time.sleep(3)

            get_results_request = requests.get(
                f'https://operation.api.cloud.yandex.net/operations/{trans_req_json["id"]}',
                headers=API_AUTH_HEADER
            )

            results = json.loads(get_results_request.content)
            logging.debug(f'Checked results of transcribation, is done? - {results["done"]}')

            if results['done'] is True:
                self.join_long_text(results['response']['chunks'])
                self.delete_from_object_storage()
                break

            else:
                retries += 1

        if retries >= retries_amount:
            logging.WARNING(f'Retries of transcribing long message exceeded')
            self.delete_from_object_storage()

    def check_db(self):
        db_connection = sqlite3.connect(f'{DB_NAME}.db')
        cursor = db_connection.cursor()
        db_result = cursor.execute(f'''
                                    SELECT transcribation 
                                    FROM records_transcribations 
                                    WHERE record_id = '{self.voice_id}'
                                    ''').fetchone()
        db_connection.commit()
        db_connection.close()

        logging.debug(f'Checked db for {self.voice_id} found - {db_result}')

        if db_result is not None:
            logging.info(f'Found record for {self.voice_id} in DB, sending from there - {db_result[0]}')
            self.transcribed_text = db_result[0]

    def add_to_db(self):
        db_connection = sqlite3.connect(f'{DB_NAME}.db')
        cursor = db_connection.cursor()
        cursor.execute('INSERT INTO records_transcribations VALUES (?,?)', [self.voice_id, self.transcribed_text])
        db_connection.commit()
        db_connection.close()
        logging.info(f'Saved voice_id - {self.voice_id} with text - \'{self.transcribed_text}\' to DB')

    def transcribe(self):

        reply_msg = bot.reply_to(self.message, 'Слушаю, печатаю и повинуюсь....')
        bot.send_chat_action(self.message.chat.id, 'typing')

        self.check_db()

        if self.transcribed_text is None:
            if self.duration < 30:
                self.transcribe_short()
            else:
                self.transcribe_long()
            self.add_to_db()

        if self.transcribed_text is not None:
            head = 'Вот что было сказанно в войсе\n\n' if len(self.transcribed_text) > 0 else 'Не удалось разобрать :('
            reply_text = f'{head}{self.transcribed_text}'

            bot.edit_message_text(reply_text,
                                  reply_msg.chat.id,
                                  reply_msg.message_id)
        else:
            bot.edit_message_text('Сорри, я ушной или с твоим голосом что-то не так, что моя нейросетка не понимает',
                                  reply_msg.chat.id,
                                  reply_msg.message_id)
