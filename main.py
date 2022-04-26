#!/usr/bin/env python3
from lib.raw_event_processor import RawEventProcessor
from lib import utils
import psycopg2
from psycopg2.extras import RealDictCursor

def connect(config):
    if config is None:
        return None

    connection = psycopg2.connect(
        database=config['database'],
        host=config['host'],
        user=config['user'],
        password=config['password'],
        port=config['port']
    )
    cur = connection.cursor(cursor_factory=RealDictCursor)
    return cur

def main():
    args = utils.parse_args()
    raw_event_database_config = args.raw
    pending_session_database_config = args.pending
    custom_session_database_config = args.custom

    raw_events_cur = connect(raw_event_database_config)
    pending_session_cur = connect(pending_session_database_config)
    custom_session_cur = connect(custom_session_database_config)

    processor = RawEventProcessor(raw_events_cur, pending_session_cur, custom_session_cur)
    processor.process_raw_events()


if __name__ == '__main__':
    main()