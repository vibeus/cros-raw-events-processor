from datetime import timedelta
from .utils import get_logger
import psycopg2
from psycopg2.extras import RealDictCursor
import json

LOGGER = get_logger()

IDLE_TIME = timedelta(seconds=600)
SESSION_START = 'SessionStart'
SESSION_END = 'SessionEnd'

class Error(Exception):
    """Base class for other exceptions"""
    pass

class UnreachableBlockError(Exception):
    """Raised when this block of code is executed"""
    pass

class UnmatchedPendingSessionError(Error):
    """Raised when multiple pending sessions are stored for a single serial"""
    pass

class DatabaseOutOfSyncError(Error):
    """Raised when table in database is out of sync"""
    pass

class State():
    """
    Possible values:
    1. REAL_IDLE:
        This is different from system idle event which is fired when there is no user input during given time interval.
    2. PLAYING_VIDEO
    3. WAIT_INPUT
    """
    REAL_IDLE = 1
    PLAYING_VIDEO = 2
    WAIT_INPUT = 3

class RawEventProcessor():
    state_bookmark_key = 'max_raw_event_receiving_time'
    raw_event_bookmark_key = 'collector_tstamp'

    def __init__(self, raw_events_config, cros_sessions_config, intermediate_storage_config, last_processor_state, debug):
        LOGGER.info("Initiate RawEventProcessor.")
        self.last_processor_state = last_processor_state
        self.current_proccesor_state = {}
        self.last_max_raw_event_receiving_time = (last_processor_state or {}).get(RawEventProcessor.state_bookmark_key) or raw_events_config['start_date']
        self.debug = debug

        self.last_event = None
        self.last_pending_session_event = None
        self.last_pending_session_info = None

        self.raw_events_cur = self.connect_postgres(raw_events_config)
        self.cros_sessions_cur = self.connect_postgres(cros_sessions_config)
        self.intermediate_storage_cur = self.cros_sessions_cur if intermediate_storage_config is None else self.connect_postgres(intermediate_storage_config)

        self.pending_sessions = {}
        self.pending_sessions_sql_tasks = {}

        select_new_raw_events_sql = f"""
        SELECT
            ctx.serial,
            ctx.user_id,
            ae.action,
            e.derived_tstamp AS tstamp,
            ctx.session_id,
            ctx.session_type,
            e.collector_tstamp
        FROM
            atomic.us_vibe_cros_action_event_1 ae
            JOIN atomic.us_vibe_cros_event_context_1 ctx ON ae.root_id = ctx.root_id
            JOIN atomic.events e ON e.event_id = ctx.root_id
        WHERE
            e.{RawEventProcessor.raw_event_bookmark_key} > '{self.last_max_raw_event_receiving_time}' -- Use collector_tstamp here
            AND ctx.serial NOT LIKE '%OEM%'
        ORDER BY ctx.serial, e.derived_tstamp, ae.action
        """
        self.raw_events_cur.execute(select_new_raw_events_sql)
        self.raw_events_rows = self.raw_events_cur.fetchall()
        LOGGER.info(f"{len(self.raw_events_rows)} raw events found.")

        self.intermediate_storage_cur.execute("CREATE SCHEMA IF NOT EXISTS cros_derived")
        create_pending_sessions_table_sql = """
        CREATE TABLE IF NOT EXISTS cros_derived.pending_sessions (
            serial          VARCHAR(128)    PRIMARY KEY,
            user_id         VARCHAR(128)    NOT NULL,
            raw_session_id  VARCHAR(128)    NOT NULL,
            start_time      TIMESTAMP       NOT NULL,
            last_event_time TIMESTAMP       NOT NULL,
            session_type    VARCHAR(128)    NOT NULL,
            last_state      VARCHAR(128)    NOT NULL,
            split_counter   INT             NOT NULL
        )
        """
        self.intermediate_storage_cur.execute(create_pending_sessions_table_sql)

        self.cros_sessions_cur.execute("CREATE SCHEMA IF NOT EXISTS cros_derived")
        create_cros_sessions_table_sql = """
        CREATE TABLE IF NOT EXISTS cros_derived.cros_sessions (
            serial          VARCHAR(128)    NOT NULL,
            user_id         VARCHAR(128)    NOT NULL,
            tstamp          TIMESTAMP       NOT NULL,
            session_id      VARCHAR(128)    NOT NULL,
            session_type    VARCHAR(128)    NOT NULL,
            action          VARCHAR(128)    NOT NULL
        )
        """
        self.cros_sessions_cur.execute(create_cros_sessions_table_sql)

        select_pending_sessions_sql = """
        SELECT
            serial,
            user_id,
            raw_session_id,
            start_time,
            last_event_time,
            session_type,
            last_state,
            split_counter
        FROM
            cros_derived.pending_sessions
        """
        self.intermediate_storage_cur.execute(select_pending_sessions_sql)
        self.pending_sessions_rows = self.intermediate_storage_cur.fetchall()

        for pending_session in self.pending_sessions_rows:
            serial = pending_session['serial']
            if self.pending_sessions.get(serial) is not None:
                raise UnmatchedPendingSessionError
            self.pending_sessions[serial] = dict(pending_session)

    def connect_postgres(self, config):
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

    def drop_tables(self):
        self.drop_cros_sessions()
        self.drop_intermediate_storage()

    def drop_cros_sessions(self):
        drop_table_sql = "DROP TABLE IF EXISTS cros_derived.cros_sessions"
        self.cros_sessions_cur.execute(drop_table_sql)
        self.cros_sessions_cur.connection.commit()
        LOGGER.info("Drop cros_derived.cros_sessions")

    def drop_intermediate_storage(self):
        drop_table_sql = "DROP TABLE IF EXISTS cros_derived.pending_sessions"
        self.intermediate_storage_cur.execute(drop_table_sql)
        self.intermediate_storage_cur.connection.commit()
        LOGGER.info("Drop cros_derived.pending_sessions")

    def change_session_state(self, current_event):
        serial = current_event['serial']
        pending_session = self.pending_sessions.get(serial)
        if pending_session is None:
            """
            This could happen when we first encouter AutoEndSession and then receive ExitSession
            immediately after. At the time when we are processing ExitSession, there is no pending
            session out there.
            """
            return

        if pending_session['raw_session_id'] != current_event['session_id']:
            raise UnmatchedPendingSessionError

        old_state = pending_session['last_state']
        current_event_type = current_event['action']
        current_event_time = current_event['tstamp']

        updates = {}

        if current_event_type in ['ExitSession', 'AutoEndSession']:
            if old_state != State.REAL_IDLE:
                self.insert_cros_session_into_database(pending_session, SESSION_END, update_pending_session=False)
            self.delete_pending_session(pending_session)

        elif old_state == State.REAL_IDLE:
            split_counter = pending_session['split_counter']
            if current_event_type in ['ExistSession', 'AutoEndSession']:
                raise UnreachableBlockError
            elif current_event_type == 'Idle':
                """Two consecutive Idle events"""
                pass
            else:
                updates.update({
                    'last_event_time': current_event_time,
                    'last_state': State.PLAYING_VIDEO if current_event_type in ['VideoStart', 'AudioStart'] else State.WAIT_INPUT,
                    'split_counter': split_counter + 1
                })
                self.update_pending_session_dict(pending_session, updates)
                self.insert_cros_session_into_database(pending_session, SESSION_START)

        elif old_state == State.PLAYING_VIDEO:
            updates.update({
                'last_event_time': current_event_time
            })
            if current_event_type in ['AudioEnd', 'VideoEnd']:
                updates.update({
                    'last_state': State.WAIT_INPUT
                })
            else:
                """The session does not end in this case."""
                updates.update({
                    'last_state': State.PLAYING_VIDEO
                })

            self.update_pending_session_dict(pending_session, updates)

        elif old_state == State.WAIT_INPUT:
            if current_event_type == 'Idle':
                updates.update({
                    'last_event_time': current_event_time - IDLE_TIME,
                    'last_state': State.REAL_IDLE
                })
                self.update_pending_session_dict(pending_session, updates)
                self.insert_cros_session_into_database(pending_session, SESSION_END)
            elif current_event_type in ['VideoStart', 'AudioStart']:
                updates.update({
                    'last_event_time': current_event_time,
                    'last_state': State.PLAYING_VIDEO
                })
                self.update_pending_session_dict(pending_session, updates)
            else:
                updates.update({
                    'last_event_time': current_event_time
                })
                self.update_pending_session_dict(pending_session, updates)

    def update_pending_session_dict(self, pending_session, updates={}):
        """
        pending_session is a dict ref inside self.pending_sessions, and thus any updates made to it will
        be reflected in self.pending_sessions.
        """
        pending_session.update(updates)
        serial = pending_session['serial']
        if self.pending_sessions.get(serial) is None:
            self.pending_sessions[serial] = pending_session

    def process_current_event(self, current_event):
        switch_raw_session = self.last_event is None or current_event['session_id'] != self.last_event['session_id']
        if not switch_raw_session:
            self.change_session_state(current_event)
        else:
            LOGGER.info("--------------------------------------------------------------------------------------------------------------------------")
            LOGGER.info(f"Start to process raw session with serial={current_event['serial']} and id={current_event['session_id']}.")
            """
            switch_raw_session == True

            1. If last raw session we processed is still pending, i.e. found in self.pending_sessions dict,
            then we store it or update it in database. We call self.process_last_session to do this.

            2. We check if current serial is asscoiated with any pending session.

            If there is no such pending session, initiate a new pending session.
            If there is a such pending session (in self.pending_sessions), there are still two cases:

            a) They have same raw_session_id. In this case, we simply call change_session_state function.

            b) They have different raw_session_id. For this scenrio, we do different stuff based on the status
            of that pending session we stored before:

                * REAL_IDLE: This means we have already sent end event for last cros session and it will
                only start a new cros session after a meaningful event occurs for that raw session id.
                However, that's not the case here since we encourter a new raw session. Thus, we simply
                delete previous session without sending any event.

                * Not REAL_IDLE: This means the session is indeed ongoing and we need to end it and send
                end event.

                Then for b), we delete pending session and initiate a new one.
            """
            if self.last_event is not None:
                self.process_last_session(self.last_event)
            current_serial = current_event['serial']
            pending_session_with_same_serial = self.pending_sessions.get(current_serial)
            if pending_session_with_same_serial:
                same_raw_session_id = pending_session_with_same_serial['raw_session_id'] == current_event['session_id']
                if same_raw_session_id:
                    """Case 1: Same serial, same raw_session_id"""
                    LOGGER.info("This is a pending session. Continue from existing one.")
                    self.change_session_state(current_event)
                else:
                    """Case 2: Same serial, different raw_session_id"""
                    last_state = pending_session_with_same_serial['last_state']
                    LOGGER.info(f"This serial associates with a different pending session with id={pending_session_with_same_serial['raw_session_id']} and state={last_state}.")
                    if last_state == State.REAL_IDLE:
                        pass
                    else:
                        LOGGER.info("Create SessionEnd event for the pending session since its state is not REAL_IDLE.")
                        self.insert_cros_session_into_database(pending_session_with_same_serial, SESSION_END, update_pending_session=False)

                    self.delete_pending_session(pending_session_with_same_serial)
                    self.initiate_pending_session(current_event)
            else:
                """No pending session with same serial. Thus initiate a new one."""
                LOGGER.info("No pending session with same serial. Thus initiate a new one.")
                self.initiate_pending_session(current_event)
            LOGGER.info(f"Finish processing the first event in this batch for raw session with serial={current_event['serial']} and id={current_event['session_id']}.")
        new_state_bookmark_value = max(self.current_proccesor_state.get(RawEventProcessor.state_bookmark_key, ''), str(current_event[RawEventProcessor.raw_event_bookmark_key]))
        self.update_processor_state({ RawEventProcessor.state_bookmark_key: new_state_bookmark_value })

    def process_raw_events(self):
        LOGGER.info("Start to process raw events.")
        i = 1
        for row in self.raw_events_rows:
            LOGGER.info(f"Processing raw event {i}")
            i += 1
            current_event = dict(row)
            self.process_current_event(current_event)
            self.last_event = current_event

        self.process_last_session(self.last_event)
        self.finish()

    def print_cros_sessions(self):
        self.cros_sessions_cur.execute("SELECT * FROM cros_derived.cros_sessions")
        rows = self.cros_sessions_cur.fetchall()
        for row in rows:
            print(row)

    def print_pending_sessions(self):
        self.intermediate_storage_cur.execute("SELECT * FROM cros_derived.pending_sessions")
        rows = self.intermediate_storage_cur.fetchall()
        for row in rows:
            print(row)

    def process_last_session(self, last_event):
        """
        Step 1 in self.process_current_event function
        If last raw session we processed is still pending, i.e. found in self.pending_sessions dict,
        then we store it or update it in database.
        """
        serial = last_event['serial']
        id = last_event['session_id']
        LOGGER.info(f"Processing last session with serial={serial} and id={id}.")
        pending_session = self.pending_sessions.get(serial)
        if pending_session is not None:
            LOGGER.info("Found pending session.")
            if last_event['session_id'] != pending_session['raw_session_id']:
                raise UnmatchedPendingSessionError
            row_count = self.get_pending_session_count_from_database(pending_session)
            if row_count != 1:
                raise DatabaseOutOfSyncError
            self.update_pending_session_in_database(pending_session)
        else:
            """
            If there is no pending session, it means we have done everything with regard to last session.
            """
            LOGGER.info("Does not found any pending session. We have done everything regarding last session.")

    def get_pending_session_count_from_database(self, session):
        sql = f"SELECT * FROM cros_derived.pending_sessions WHERE serial = '{session['serial']}' LIMIT 10"
        self.intermediate_storage_cur.execute(sql)
        row_count = self.intermediate_storage_cur.rowcount
        return row_count

    def build_cros_session_id(self, session):
        return f"{session['raw_session_id']}/{session['split_counter']}"

    def insert_cros_session_into_database(self, session, start_or_end, update_pending_session=True):
        """
        First update session in cros_derived.pending_sessions, and then store the following fields from
        session into cros sessions table.
        """
        cros_sessions_columns = 'serial, user_id, session_id, tstamp, session_type, action'

        if update_pending_session:
            self.update_pending_session_in_database(session)

        cros_session_id = self.build_cros_session_id(session)
        sql = f"""
        INSERT INTO cros_derived.cros_sessions ({cros_sessions_columns})
        VALUES ('{session['serial']}', '{session['user_id']}', '{cros_session_id}', '{session['last_event_time']}', '{session['session_type']}', '{start_or_end}')
        """
        self.cros_sessions_cur.execute(sql)
        LOGGER.info(sql)

    def initiate_pending_session(self, current_event):
        """
        Initiate pending session in cros_derived.pending_sessions and also store the cros SessionStart event.
        """
        current_event_type = current_event['action']
        if current_event_type in ['ExitSession', 'AutoEndSession']:
            """Do not include Idle event type here"""
            return

        session = {
            'serial': current_event['serial'],
            'user_id': current_event['user_id'],
            'raw_session_id': current_event['session_id'],
            'start_time': current_event['tstamp'],
            'last_event_time': current_event['tstamp'],
            'session_type': current_event['session_type'],
            'split_counter': 1
        }
        if current_event_type in ['Idle']:
            new_state = State.REAL_IDLE
        elif current_event_type in ['StartVideo', 'StartAudio']:
            new_state = State.PLAYING_VIDEO
        else:
            new_state = State.WAIT_INPUT
        session.update({
            'last_state': new_state
        })

        row_count = self.get_pending_session_count_from_database(session)
        if row_count == 0:
            self.update_pending_session_dict(session)
            self.insert_pending_session_into_database(session)
            if new_state != State.REAL_IDLE:
                """
                This is a weird scenario when Idle event is the first in a raw session we encouter.
                We don't send SessionStart in this case but we do initiate a pending session.
                """
                self.insert_cros_session_into_database(session, SESSION_START, update_pending_session=False)
        else:
            raise DatabaseOutOfSyncError

    def insert_pending_session_into_database(self, session):
        pending_sessions_columns = 'serial, user_id, raw_session_id, start_time, last_event_time, session_type, last_state, split_counter'
        sql = f"""
        INSERT INTO cros_derived.pending_sessions ({pending_sessions_columns})
        VALUES ('{session['serial']}', '{session['user_id']}', '{session['raw_session_id']}', '{session['start_time']}', '{session['last_event_time']}', '{session['session_type']}', '{session['last_state']}', '{session['split_counter']}')
        """
        self.intermediate_storage_cur.execute(sql)
        LOGGER.info(sql)

    def update_pending_session_in_database(self, session):
        sql = f"""
        UPDATE cros_derived.pending_sessions
        SET
            last_event_time = '{session['last_event_time']}',
            last_state = '{session['last_state']}',
            split_counter = {session['split_counter']}
        WHERE serial = '{session['serial']}' AND raw_session_id = '{session['raw_session_id']}'
        """
        self.intermediate_storage_cur.execute(sql)
        LOGGER.info(sql)

    def delete_pending_session(self, session):
        serial = session['serial']
        raw_session_id = session['raw_session_id']
        if self.pending_sessions.get(serial, {}).get('raw_session_id') != raw_session_id:
            raise UnmatchedPendingSessionError
        sql = f"DELETE FROM cros_derived.pending_sessions WHERE serial = '{serial}'"
        self.intermediate_storage_cur.execute(sql)
        del self.pending_sessions[serial]
        LOGGER.info(sql)

    def update_processor_state(self, updates):
        self.current_proccesor_state.update(updates)

    def finish(self):
        """
        1. Commit database changes.
        2. Write new state.
        """
        if not self.debug:
            self.intermediate_storage_cur.connection.commit()
            if self.intermediate_storage_cur != self.cros_sessions_cur:
                self.cros_sessions_cur.connection.commit()
        print(json.dumps(self.current_proccesor_state))