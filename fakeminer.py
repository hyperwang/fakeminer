#!/usr/bin/python3

import asyncio
import asyncore
import socket
import random
import json
import logging
import time
import functools
import time

logging.basicConfig(level=logging.DEBUG)

UNIT_MAP = {
    'G': 1,
    'T': 1000,
    'P': 1000000,
}


def rate2pow(rate):
    assert isinstance(rate, tuple) and len(rate) == 2
    assert len(rate) == 2
    rate_num, unit = rate
    rate_in_g = rate_num * UNIT_MAP[unit.upper()]
    return int(rate_in_g/4.295)


class StratumJob():
    def __init__(self, pool, user, params):
        self.pool = pool
        self.user = user
        self.job_id, self.prevhash, self.coinb1, self.coinb2, self.merkle_branch, \
        self.version, self.nbits, self.ntime, self.clean_jobs = params

    def __str__(self):
        return "job_id:%s, prevhash:%s, branch_size:%d, clean_jobs:%s"%(self.job_id, self.prevhash, len(self.merkle_branch), self.clean_jobs)


class StratumSession():
    def __init__(self, client, rate):
        self.client = client
        self.rate = rate
        self.diff = 1
        self.exn1 = 0
        self.exn2_size = 4
        self.job = None
        self.pow_per_sec = rate2pow(self.rate)
        self.share_list = []

    def set_diff(self, diff):
        assert isinstance(diff, int)
        self.diff = diff

    def set_extra_nounce(self, exn1, exn2_size):
        self.exn1 = exn1
        self.exn2_size = exn2_size
        logging.debug("Set extra nounce 1 value=%s, extra nounce 2 size=%s", self.exn1, self.exn2_size)
    
    def set_job(self, job_params):
        self.job = StratumJob(self.client.pool, self.client.user, job_params)

    def should_submit(self):
        now_ts = int(time.time())
        for ts, diff in self.share_list:
            if now_ts - ts > 60:
                self.share_list.remove((ts, diff))
        pow_in_minute = sum([diff for _, diff in self.share_list]) 
        target_pow = self.pow_per_sec * 60
        logging.debug("diff: %d, pow in minute: %d, target pow in minute: %d", self.diff, pow_in_minute, target_pow)
        return pow_in_minute < target_pow 
    
    def submit_share(self):
        self.share_list.append((int(time.time()), self.diff))

    def __str__(self):
        return "pool(%s),user(%s),pow(%d),job(%s)"%(self.client.pool, self.client.user, self.pow_per_sec, self.job)
    

class StratumClientProtocol(asyncio.Protocol):
    STATE_IDLE = 0b00000000
    STATE_CONN = 0b00000001
    STATE_SUBS = 0b00000010 
    STATE_AUTH = 0b00000100 
    STATE_NOTI = 0b00001000 
    STATE_DIFF = 0b00010000 
    STATE_OK   = 0b11111111

    def __init__(self, loop, pool, user, worker, passwd, rate):
        self.loop = loop
        self.pool, self.user, self.worker, self.passwd = pool, user, worker, passwd
        self.worker_name = "%s.%s"%(self.user, self.worker)
        self.stratum_id = 0
        self.subs_stratum_id = 0
        self.auth_stratum_id = 0
        self.state = StratumClientProtocol.STATE_IDLE
        self.session = StratumSession(self, rate)
        self.timeout = 30

    def _stratum_send_request(self, transport, method, params=[]):
        self.stratum_id += 1
        msg = json.dumps({'id': self.stratum_id, 'method': method, 'params': params}) + '\n'
        transport.write(msg.encode())
        logging.debug("Send request, Pool: %s, Msg: %s", self.pool, msg[0:-1])
        return self.stratum_id

    def stratum_send_subscribe(self, transport):
        self.subs_stratum_id = self._stratum_send_request(transport, 'mining.subscribe', [])
    
    def stratum_send_authorize(self, transport):
        self.auth_stratum_id = self._stratum_send_request(transport, 'mining.authorize', [self.worker_name, self.passwd])
    
    def stratum_send_submit(self, transport):
        ntime = "%0.8x"%int(time.time())
        if self.session.job is None:
            return
        job_id = self.session.job.job_id
        exn2 = ''.join([random.choice('0123456789abcdef') for _ in range(self.session.exn2_size * 2)])
        nounce = ''.join([random.choice('0123456789abcdef') for _ in range(8)])
        self._stratum_send_request(transport, 'mining.submit', [self.worker_name, job_id, exn2, ntime, nounce])
        self.session.submit_share()

    def connection_made(self, transport):
        self.transport = transport
        self.state = StratumClientProtocol.STATE_CONN
        self.stratum_send_subscribe(transport)
        self.stratum_send_authorize(transport)

    def _handle_mining_notify(self, msg):
        self.session.set_job(msg['params']) 
        logging.debug("MINING_NOTIFY: %s", self.session.job)
    
    def _handle_mining_set_difficulty(self, msg):
        self.session.set_diff(int(msg['params'][0]))

    def _handle_mining_auth_response(self, msg):
        try:
            return msg['result']
        except ValueError:
            return False
    
    def _handle_mining_subs_response(self, msg):
        try:
            _, exn1, exn2_size = msg['result']
            self.session.set_extra_nounce(exn1, int(exn2_size))
            return True
        except ValueError:
            return False

    def handle_remote_request(self, msg):
        logging.debug("Receive remote request, Pool: %s, Msg: %s", self.pool, json.dumps(msg))
        if msg['method'] == 'mining.notify' and self.state & StratumClientProtocol.STATE_SUBS > 0:
            self._handle_mining_notify(msg)
            self.state |= StratumClientProtocol.STATE_NOTI
        if msg['method'] == 'mining.set_difficulty' and self.state & StratumClientProtocol.STATE_SUBS > 0:
            self._handle_mining_set_difficulty(msg)
            self.state |= StratumClientProtocol.STATE_DIFF
        if self.state & (StratumClientProtocol.STATE_AUTH|StratumClientProtocol.STATE_NOTI|StratumClientProtocol.STATE_DIFF):
            self.state = StratumClientProtocol.STATE_OK

    def handle_response(self, msg):
        logging.debug("Receive response, Pool: %s, Msg: %s", self.pool, json.dumps(msg))
        if msg['id'] == self.subs_stratum_id and self.state == StratumClientProtocol.STATE_CONN:
            if self._handle_mining_subs_response(msg):
                self.state = StratumClientProtocol.STATE_SUBS
        if msg['id'] == self.auth_stratum_id and self.state & StratumClientProtocol.STATE_SUBS > 0:
            if self._handle_mining_auth_response(msg):
                self.state |= StratumClientProtocol.STATE_AUTH
    
    def do_in_period(self, transport):
        if not self.state == StratumClientProtocol.STATE_OK:
            self.timeout -= 1
        if self.session.should_submit():
            self.stratum_send_submit(transport)

    def data_received(self, data):
        msgs = data.decode().splitlines()
        for msg in msgs:
            try:
                msg = json.loads(msg)
            except ValueError:
                logging.debug("Failed on parse server message")
                continue
            if 'result' in msg:
                self.handle_response(msg)
                continue
            if 'method' in msg:
                self.handle_remote_request(msg)
                continue


@asyncio.coroutine
def send_shares(clients):
    while True:
        for transport, protocol in clients:
            protocol.do_in_period(transport)
        yield from asyncio.sleep(0.05)


loop = asyncio.get_event_loop()

miners = [
    ['Bixin', 'hyper', 's5', '123', (100, 'T')],
]

clients = []
for miner in miners:
    coro = loop.create_connection(lambda: StratumClientProtocol(loop, *miner), '127.0.0.1', 3333)
    clients.append(loop.run_until_complete(coro))

task = asyncio.Task(send_shares(clients))
loop.run_until_complete(task)

loop.run_forever()
loop.close()
