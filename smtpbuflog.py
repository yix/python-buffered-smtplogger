import datetime
import logging
import smtplib
import threading
import time
import Queue


class BufferingSMTPHandler(logging.Handler):
    """Set up a buffering SMTP logging handler."""

    # Configurable parameters
    _POLL_INTERVAL = 5       # Interval between checks for sets of new records.
    _POLL_DURATION_MAX = 10  # If a record is available, max time to continue
    #                          polling for more records.
    _SEND_INTERVAL = 2 * 60  # Interval between sends.

    # Setup class environment
    _Q = Queue.Queue()
    _LOCK = threading.Lock()
    _LAST_SEND_TIME = float('-inf')

    def state(self):
        """Return a dict containing keys and values providing information about
        the state of the handler."""

        #time_now = time.time()
        time_now = datetime.datetime.utcnow()

        # Calculate time since last email
        if self._LAST_SEND_TIME != float('-inf'):
            time_of_last = datetime.datetime.utcfromtimestamp(
                self._LAST_SEND_TIME)
            time_since_last = time_now - time_of_last
        else:
            time_since_last = '(none sent yet)'

        # Time to next earliest possible email
        if self._LAST_SEND_TIME != float('-inf'):
            time_of_next = datetime.datetime.utcfromtimestamp(
                self._LAST_SEND_TIME + self._SEND_INTERVAL)
            time_of_next = max(time_now, time_of_next)
            time_until_next = time_of_next - time_now
        else:
            time_until_next = time_now - time_now

        return {'Total number of unprocessed errors': self._Q.qsize() + self._q.qsize(),
                'Intervals': 'Poll: {}s, Send: {}s'.format(self._POLL_INTERVAL, self._SEND_INTERVAL),
                'Poll duration max': '{}s'.format(self._POLL_DURATION_MAX),
                'Time since last email': time_since_last,
                'Time to next earliest possible email': 'at least {}'.format(time_until_next),
                # This simplification doesn't account for _POLL_INTERVAL and
                # _POLL_DURATION_MAX, etc.
                'Recipients': self._header['toaddrs_str'],
                }

    def __init__(self, fromaddr, toaddrs, subject, host='localhost', TLS=False, credentials=None):

        # Setup instance environment
        self._active = True
        self._q = Queue.Queue()  # this is different from self._Q

        # Construct email header
        self._header = {'fromaddr': fromaddr,
                        'toaddrs': toaddrs,
                        'toaddrs_str': ','.join(toaddrs),
                        'subject': subject,
                        }
        self._header['header'] = 'From: {fromaddr}\r\nTo: {toaddrs_str}\r\nSubject: {subject}\r\n\r\n'.format(
            **self._header)
        self.TLS = TLS
        self.mail_credentials = credentials
        self.mail_host = host

        # Start main buffer-processor thread
        thread_name = '{}Thread'.format(self.__class__.__name__)
        # Note: The class is intentionally not inherited from threading.Thread,
        #       as doing so was found to result in the target thread not being
        #       named correctly, possibly due to a namespace collision.
        thread = threading.Thread(target=self.run, name=thread_name)
        thread.daemon = True
        thread.start()

        super(BufferingSMTPHandler, self).__init__()

    def close(self):
        """Process some remaining records."""

        super(BufferingSMTPHandler, self).close()

        self._active = False
        self._POLL_DURATION_MAX = min(0.25, self._POLL_DURATION_MAX)
        # no need to set self.__class__._POLL_DURATION_MAX
        self._process_recordset()

    def emit(self, record):
        """Queue a record into the class queue so it can be emitted
        collectively."""

        # This method can be called by various threads.
        self._Q.put(self.format(record))

    def run(self):
        """Periodically flush the buffer."""

        while self._active:
            with self._LOCK:  # protects _LAST_SEND_TIME and _q
                next_send_time = self._LAST_SEND_TIME + self._SEND_INTERVAL
                if time.time() > next_send_time:
                    self._process_recordset()
                    sleep_time = self._POLL_INTERVAL
                else:
                    # assert (next_send_time != -inf)
                    sleep_time = max(next_send_time - time.time(), 0)
            time.sleep(sleep_time)

    def _process_recordset(self):
        """Process a set of records buffered in class queue."""

        try:
            self._move_recordset_from_Q_to_q()
            if not self._q.empty():
                self._send_records_from_q()
                self.__class__._LAST_SEND_TIME = time.time()
        except (KeyboardInterrupt, SystemExit):
            pass

    def _move_recordset_from_Q_to_q(self):
        """Move a set of records from class queue to instance queue."""

        deadline = time.time() + self._POLL_DURATION_MAX
        while time.time() < deadline:
            try:
                self._q.put(self._Q.get_nowait())
                self._Q.task_done()
            except Queue.Empty:
                if self._q.empty():
                    break
                time.sleep(0.1)

    def _send_records_from_q(self):
        """Send records that are in instance queue."""

        records = []
        try:

            # Get formatted records from instance queue
            while True:
                records.append(self._q.get_nowait())
                self._q.task_done()

        except (Queue.Empty, KeyboardInterrupt, SystemExit):
            pass
        finally:

            # Send formatted records from instance queue
            if records:

                body = 'Included messages: {}\r\n'.format(len(records))

                num_pending_messages = self._Q.qsize() + self._q.qsize()
                if num_pending_messages > 0:
                    body += 'Pending messages:  {}\r\n'.format(
                        num_pending_messages)

                # Add main content of message body
                body += '\r\n'
                body += '\r\n'.join(records)
                msg = self._header['header'] + body
                smtp = smtplib.SMTP(self.mail_host)
                if self.TLS:
                    smtp.starttls()
                if self.mail_credentials is not None:
                    smtp.login(self.mail_credentials['username'],
                              self.mail_credentials['password'])
                smtp.sendmail(self._header['fromaddr'],
                              self._header['toaddrs'], msg)
                smtp.quit()


def test():
    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)
    sh = BufferingSMTPHandler(
        fromaddr, toaddrs, subject, mailhost, TLS=True, credentials=creds)
    ch = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-7s] (%(threadName)-12s) %(message)s')
    sh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(sh)
    logger.addHandler(ch)

    for i in xrange(12):
        logger.info("Info index = %d", i)
    time.sleep(30)
    for i in xrange(130):
        logger.info("Info index = %d", i)
        time.sleep(1)
    time.sleep(150)
    for i in xrange(30):
        logger.info("Info index = %d", i)
    time.sleep(150)
    # logging.shutdown()

mailhost = 'smtp.gmail.com:587'
fromaddr = 'test@gmail.com'
toaddrs = ['test@gmail.com', ]
creds = {'username': 'test@gmail.com',
         'password': '**************'}
subject = 'Logmailer summary'

if __name__ == "__main__":
    test()
