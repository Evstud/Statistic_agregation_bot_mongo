from aiogram import Bot, Dispatcher, executor, types
import json
import datetime
from main import main, client
from dotenv import load_dotenv
import os

load_dotenv()

bot = Bot(token=os.getenv("api_token"))
dp = Dispatcher(bot)


@dp.message_handler(commands=['start', 'help'])
async def send_first_phrase(message: types.Message):
    text = 'Введите запрос. Пример: {"dt_from": "2022-09-01T00:00:00", ' \
           '"dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
    await message.reply(text)

@dp.message_handler()
async def handle_message(message: types.Message):
    wrong_response = 'Допустимо отправлять только следующие запросы: {"dt_from": "2022-09-01T00:00:00", "dt_upto":' \
                     ' "2022-12-31T23:59:00", "group_type": "month"}, {"dt_from": "2022-10-01T00:00:00", "dt_upto": ' \
                     '"2022-11-30T23:59:00", "group_type": "day"}, {"dt_from": "2022-02-01T00:00:00", "dt_upto": ' \
                     '"2022-02-02T00:00:00", "group_type": "hour"}'
    result_keys = ["group_type", "dt_from", "dt_upto"]
    group_types = ["month", "week", "day", "hour"]
    try:
        result = json.loads(str(message.text))
        if set(result.keys()) == set(result_keys):
            if result["group_type"] in group_types:
                if type(datetime.datetime.fromisoformat(result["dt_upto"])) ==\
                        type(datetime.datetime.fromisoformat("2022-10-01T00:00:00"))\
                        and type(datetime.datetime.fromisoformat(result["dt_from"]))\
                        == type(datetime.datetime.fromisoformat("2022-10-01T00:00:00")):

                        group_type = result["group_type"]
                        dt_from = result["dt_from"]
                        dt_upto = result["dt_upto"]

                        try:
                            msg_to_return = await main(
                                db_name=os.getenv("db_name"),
                                collection_name=os.getenv("collection_name"),
                                client=client,
                                group_type=group_type,
                                dt_from=dt_from,
                                dt_upto=dt_upto
                            )
                            await message.reply(msg_to_return)
                        except Exception as main_ex:
                            print(f"Something is wrong: {main_ex}")
                else:
                    await message.reply(wrong_response)
            else:
                await message.reply(wrong_response)
        else:
            await message.reply(wrong_response)
    except Exception as _ex:
        print(_ex)
        print(wrong_response)
        await message.reply(wrong_response)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
