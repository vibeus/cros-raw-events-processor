#!/usr/bin/env python3
from lib.raw_event_processor import RawEventProcessor
from lib import utils

def main():
    args = utils.parse_args()
    raw_events_config = utils.expand_env(args.raw)
    intermediate_storage_config = utils.expand_env(args.intermediate)
    cros_sessions_config = utils.expand_env(args.cros)

    processor = RawEventProcessor(
        raw_events_config=raw_events_config,
        cros_sessions_config=cros_sessions_config,
        intermediate_storage_config=intermediate_storage_config,
        last_processor_state=args.state,
        debug=args.debug
    )
    if args.drop:
        processor.drop_tables()
    else:
        processor.process_raw_events()

if __name__ == '__main__':
    main()