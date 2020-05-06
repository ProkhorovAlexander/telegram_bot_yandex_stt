import os
import logging
import logging.handlers
from listener_bot.utils import check_buckets, init_users, init_database, bot, get_allowed_users_and_channels, \
    MAX_DURATION, VoiceMessage, add_channels, allowed_presence_check, send_greeting

# creating folder for logging if doesn't exists
logfile_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')
if not os.path.exists(logfile_path):
    os.makedirs(logfile_path)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(filename)s %(message)s',
                    handlers=[
                        logging.StreamHandler(),
                        logging.handlers.TimedRotatingFileHandler(f'{logfile_path}/bot_log.log',
                                                                  encoding='utf8',
                                                                  when='W0',
                                                                  backupCount=5)
                             ])


# BOT starter handler
@bot.message_handler(commands=['start_voice_bot'])
def start_command_bot(message):
    allowed_users, allowed_channels = get_allowed_users_and_channels()

    if message.chat.id not in allowed_channels:
        if message.from_user.id not in allowed_users:
            bot.reply_to(message, 'Вы не можете стартовать бота')

            logging.info(
                f'Username {message.from_user.username} (id {message.from_user.id} tried to start bot in chat ID {message.chat.id}')

        elif message.from_user.id in allowed_users:
            add_channels(message.chat.id)
            send_greeting(message.chat.id)

    elif message.chat.id in allowed_channels:
        bot.reply_to(message, 'Я уже повинуюсь, господин')

    else:
        bot.reply_to(message, 'ВАТАФФФ')


@bot.message_handler(content_types=['voice'])
def voice_handler_func(message):
    allowed_users, allowed_channels = get_allowed_users_and_channels()

    if message.chat.id not in allowed_channels or allowed_presence_check(allowed_users, message.chat.id) is False:
        return

    if message.voice.duration > MAX_DURATION:
        bot.reply_to(message, 'Голосовуха больше 5 минут? Я конечно хочу служить, но не настолько.')
        return

    voice = VoiceMessage(message)
    voice.transcribe()


def main():
    check_buckets()
    init_users()
    init_database()

    bot.polling(none_stop=False, timeout=150, interval=2)


if __name__ == '__main__':
    main()
