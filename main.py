import threading
import time
import random
import requests
import json

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

NODES = ["localhost:8000", "localhost:8001", "localhost:8002", "localhost:8003", "localhost:8004"]

lock = threading.Lock()

TIMEOUT = 1


class Node:
    def __init__(self, address):
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_length = 0
        self.current_role = 'follower'
        self.votes_received = set()
        self.sent_length = dict()
        self.acked_length = dict()
        self.current_leader = None

        self.node_id = address
        self.address = address

        self.election_timer = None
        self.election_timeout = None
        self.replicate_log_period = None
        self.last_leader_heartbeat = None
        self.leader_heartbeat_check_period = None

        threads = []
        threads.append(self.start_periodic_replicate_log())
        threads.append(self.start_check_leader_heartbeat())

        server = HTTPServer((self.address.split(':')[0], int(self.address.split(':')[1])), RequestHandler)
        server.node = self
        server.serve_forever()

        print(f"Node {self.address} started as follower.")
        for thread in threads:
            thread.join()
    
    def start_election_timer(self):
        self.cancel_election_timer()
        self.election_timeout = random.uniform(5.0, 10.0)
        self.election_timer = threading.Timer(self.election_timeout, self.on_leader_failure_or_election_timeout)
    
    def cancel_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.cancel()
            self.election_timer = None
            self.election_timeout = None
    
    def periodic_replicate_log(self):
        while True:
            with lock:
                if self.current_role == 'leader':
                    for follower in NODES:
                        if follower != self.node_id:
                            print('Replicating log!!')
                            self.replicate_log(self.node_id, follower)
            #
            time.sleep(self.replicate_log_period)
    
    def start_periodic_replicate_log(self):
        self.replicate_log_period = random.uniform(1.0, 2.0)
        return threading.Thread(target=self.periodic_replicate_log).start()

    def periodic_leader_heartbeat_check(self):
        while True:
            need_election = False
            with lock:
                if self.current_role != 'leader' and self.last_leader_heartbeat + self.leader_heartbeat_check_period < time.time():
                    need_election = True

            if need_election:
                print('checking leader')
                self.on_leader_failure_or_election_timeout()
            time.sleep(self.leader_heartbeat_check_period)

    def start_check_leader_heartbeat(self):
        self.leader_heartbeat_check_period = random.uniform(10.0, 20.0)
        self.last_leader_heartbeat = time.time()
        return threading.Thread(target=self.periodic_leader_heartbeat_check).start()

    def on_leader_failure_or_election_timeout(self):
        with lock:
            self.current_term = self.current_term + 1
            self.current_role = 'candidate'
            print('Became candidate!!')
            self.voted_for = self.node_id
            self.votes_received = set([self.node_id])
            last_term = 0
            if len(self.log) > 0:
                last_term = self.log[len(self.log) - 1]['term']
            for node in NODES:
                if node != self.node_id:
                    try:
                        requests.post(f"http://{node}/request_vote", json={
                            'node_id': self.node_id,
                            'current_term': self.current_term,
                            'log_length': len(self.log),
                            'last_term': last_term
                        }, timeout=TIMEOUT)
                    except requests.RequestException as e:
                        print(f"Request vote error: {e}")
            self.start_election_timer()
    
    def on_receiving_vote_request(self, cand_id, cand_term, cand_log_length, cand_log_term):
        my_log_term = 0
        if len(self.log) > 0:
            my_log_term = self.log[len(self.log) - 1]['term']
        log_ok = (cand_log_term > my_log_term) or (cand_log_term == my_log_term and cand_log_length >= len(self.log))
        term_ok = (cand_term > self.current_term) or (cand_term == self.current_term and (self.voted_for == cand_id or self.voted_for is None))

        if log_ok and term_ok:
            self.current_term = cand_term
            self.current_role = 'follower'
            print('Became follower!!')
            self.voted_for = cand_id
            try:
                requests.post(f"http://{cand_id}/vote_response", json={
                    'node_id': self.node_id,
                    'current_term': self.current_term,
                    'ok': True
                }, timeout=TIMEOUT)
            except requests.RequestException as e:
                print(f"Response vote error: {e}")
        else:
            try:
                requests.post(f"http://{cand_id}/vote_response", json={
                    'node_id': self.node_id,
                    'current_term': self.current_term,
                    'ok': False
                }, timeout=TIMEOUT)
            except requests.RequestException as e:
                print(f"Response vote error: {e}")

    def on_receiving_vote_response(self, voter_id, term, granted):
        if self.current_role == 'candidate' and term == self.current_term and granted:
            self.votes_received.add(voter_id)
            if len(self.votes_received) > len(NODES) / 2:
                self.current_role = 'leader'
                print('Became leader!!')
                self.current_leader = self.node_id
                self.cancel_election_timer()
                for follower in NODES:
                    if follower != self.node_id:
                        self.sent_length[follower] = len(self.log)
                        self.acked_length[follower] = 0
                        with lock:
                            self.replicate_log(self.node_id, follower)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = 'follower'
            print('Became follower!!')
            self.voted_for = None
            self.cancel_election_timer()

    def on_broadcast_request(self, msg):
        if self.current_role == 'leader':
            self.log.append({'msg': msg, 'term': self.current_term})
            self.acked_length[self.node_id] = len(self.log)
            for follower in NODES:
                if follower != self.node_id:
                    with lock:
                        self.replicate_log(self.node_id, follower)
        else:
            try:
                if self.current_leader is not None:
                    requests.post(f"http://{self.current_leader}/broadcast_request", json={
                        'msg': msg
                    }, timeout=TIMEOUT)
            except requests.RequestException as e:
                print(f"Sending broadcast request to leader error: {e}")
    
    def replicate_log(self, leader_id, follower_id):
        i = self.sent_length.get(follower_id, 0)
        entries = []
        for j in range(i, len(self.log)):
            entries.append(self.log[j])
        prev_log_term = 0
        if i > 0:
            prev_log_term = self.log[i - 1]['term']
        try:
            requests.post(f"http://{follower_id}/log_request", json={
                'leader_id': leader_id,
                'current_term': self.current_term,
                'i': i,
                'prev_log_term': prev_log_term,
                'commit_length': self.commit_length,
                'entries': entries
            }, timeout=TIMEOUT)
        except requests.RequestException as e:
            print(f"Log request error: {e}")
    
    def on_receiving_log_request(self, leader_id, term, log_length, log_term, leader_commit, entries):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.current_role = 'follower'
            print('Became follower!!')
            self.current_leader = leader_id
        if term == self.current_term:
            self.current_role = 'follower'
            print('Became follower!!')
            self.current_leader = leader_id
        with lock:
            self.last_leader_heartbeat = time.time()
        log_ok = (len(self.log) >= log_length) and (log_length == 0 or log_term == self.log[log_length - 1]['term'])
        if term == self.current_term and log_ok:
            self.append_entries(log_length, leader_commit, entries)
            ack = log_length + len(entries)
            try:
                requests.post(f"http://{leader_id}/log_response", json={
                    'node_id': self.node_id,
                    'current_term': self.current_term,
                    'ack': ack,
                    'ok': True
                }, timeout=TIMEOUT)

            except requests.RequestException as e:
                print(f"Log response error: {e}")
        else:
            try:
                requests.post(f"http://{leader_id}/log_response", json={
                    'node_id': self.node_id,
                    'current_term': self.current_term,
                    'ack': 0,
                    'ok': False
                }, timeout=TIMEOUT)
            except requests.RequestException as e:
                print(f"Log response error: {e}")

    def append_entries(self, log_length, leader_commit, entries):
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length]['term'] != entries[0]['term']:
                self.log = self.log[:log_length]
        if log_length + len(entries) > len(self.log):
            for i in range(len(self.log) - log_length, len(entries)):
                self.log.append(entries[i])
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self.deliver_message(self.log[i]['msg'])
            self.commit_length = leader_commit
    
    def on_receiving_log_response(self, follower, term, ack, success):
        if term == self.current_term and self.current_role == 'leader':
            if success and ack >= self.acked_length.get(follower, 0):
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length.get(follower, 0) > 0:
                self.sent_length[follower] = self.sent_length[follower] - 1
                with lock:
                    self.replicate_log(self.node_id, follower)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = 'follower'
            print('Became follower!!')
            self.voted_for = None
    
    def acks(self, length):
        res = 0
        for node in NODES:
            if self.acked_length.get(node, 0) >= length:
                res += 1
        return res
    
    def commit_log_entries(self):
        min_acks = len(NODES) / 2 + 1
        ready = set()
        for length in range(1, len(self.log) + 1):
            if self.acks(length) >= min_acks:
                ready.add(length)
        if not len(ready) == 0 and max(ready) > self.commit_length and self.log[max(ready) - 1]['term'] == self.current_term:
            for i in range(self.commit_length, max(ready) - 1):
                self.deliver_message(self.log[i]['msg'])
            self.commit_length = max(ready)
        
    def deliver_message(self, msg):
        print(msg)

    def on_read(self, key):
        result_dict = dict()
        for i in range(0, len(self.log)):
            parts = self.log[i]['msg'].split()
            action, key_str = parts[0], parts[1]
            if len(parts) == 3:
                value_str = parts[2]
            else:
                value_str = '0'
            key = int(key_str)
            value = int(value_str)
            if action == "create":
                if key not in result_dict:
                    result_dict[key] = value
            elif action == "update":
                if key in result_dict:
                    result_dict[key] = value
            elif action == "delete":
                if key in result_dict:
                    del result_dict[key]
        return result_dict.get(key, "Error")


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        node = self.server.node
        parsed_path = urlparse(self.path)
        query = parse_qs(parsed_path.query)
        if parsed_path.path == '/status':
            node_data = {
                'node_id': node.node_id,
                'current_term': node.current_term,
                'voted_for': node.voted_for,
                'log': node.log,
                'current_role': node.current_role,
                'votes_received': list(node.votes_received),
                'sent_length': node.sent_length,
                'acked_length': node.acked_length,
                'current_leader': node.current_leader
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(bytes(json.dumps(node_data), 'utf-8'))
        elif parsed_path.path == '/read':
            content_length = int(self.headers['Content-Length'])
            get_data = json.loads(self.rfile.read(content_length))
            response = self.server.node.on_read(
                get_data['key']
            )
            if response == 'Error':
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
            else:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
            self.wfile.write(bytes(json.dumps(response), 'utf-8'))
        else:
            self.send_error(404)

    def do_POST(self):
        node = self.server.node
        parsed_path = urlparse(self.path)
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))

        if parsed_path.path == '/request_vote':
            self.server.node.on_receiving_vote_request(
                post_data['node_id'],
                post_data['current_term'],
                post_data['log_length'],
                post_data['last_term']
            )
        elif parsed_path.path == '/vote_response':
            self.server.node.on_receiving_vote_response(
                post_data['node_id'],
                post_data['current_term'],
                post_data['ok']
            )
        elif parsed_path.path == '/broadcast_request':
            self.server.node.on_broadcast_request(
                post_data['msg']
            )
        elif parsed_path.path == '/log_request':
            self.server.node.on_receiving_log_request(
                post_data['leader_id'],
                post_data['current_term'],
                post_data['i'],
                post_data['prev_log_term'],
                post_data['commit_length'],
                post_data['entries']
            )
        elif parsed_path.path == '/log_response':
            self.server.node.on_receiving_log_response(
                post_data['node_id'],
                post_data['current_term'],
                post_data['ack'],
                post_data['ok']
            )
        elif parsed_path.path == '/create':
            self.server.node.on_broadcast_request(
                'create ' + str(post_data['key'])
            )
        elif parsed_path.path == '/delete':
            self.server.node.on_broadcast_request(
                'delete ' + str(post_data['key'])
            )
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
    
    def do_PUT(self):
        node = self.server.node
        parsed_path = urlparse(self.path)
        content_length = int(self.headers['Content-Length'])
        put_data = json.loads(self.rfile.read(content_length))
        if parsed_path.path == '/update':
            self.server.node.on_broadcast_request(
                'update ' + str(put_data['key']) + ' ' + str(put_data['value'])
            )
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <port>")
    else:
        address = f"localhost:{sys.argv[1]}"
        node = Node(address)
