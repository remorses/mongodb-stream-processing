from .logger import logger
from xconfig import Config

from base64 import b64encode, b64decode

program_state = Config('./program_state.yaml', {'aggregators': []})

def read_token(id):
    logger.debug(f'reading token')
    aggregators = program_state['aggregators']
    token = aggregators[id]['last_token'] if id in aggregators else None
    last_token = b64decode(token) if token else None
    return last_token


def store_token(id, token):
    last_token = b64encode(token).decode()
    assert token == b64decode(last_token)
    program_state.write(f'aggregators.{id}', {
                        'last_token': last_token, 'id': id},)
    logger.debug(f'stored token {last_token}')