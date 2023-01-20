import threading
from enum import Enum
import zerorpc
import math


PORT = 10010


class ServerState(Enum):
    FOLLOWER = 1,
    CANDIDATE = 2,
    LEADER = 3

class RaftServer:
    def __init__(self, id):
        self.id = id
        self.term = 0
        self.voted_for = None
        self.log = []
        self.state = ServerState.FOLLOWER


    def start_election(self):  
        self.state = ServerState.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        votes_received = 1
        for server in servers:
            if server.id == self.id:
                continue

            client = zerorpc.Client()
            client.connect("tcp://{}:{}".format(server_addr_map[server], PORT))
            vote_granted = client.request_vote(self.term, self.id, len(self.log) - 1, self.log[-1].term if self.log else 0)
            if vote_granted:
                votes_received += 1
            client.close()

            if votes_received >= math.ceil(len(servers) / 2):
                self.become_leader()
                break

        
    def become_leader(self):
        self.state = ServerState.LEADER
        # TODO: broadcast leader message
    

    def become_follower(self, term):
        self.state = ServerState.FOLLOWER
        self.term = term
        self.voted_for = None

    
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.term:
            return False
        if not self.voted_for or self.voted_for == candidate_id:
            if self.log[-1].term < last_log_term or (self.log[-1].term == last_log_term and len(self.log) - 1 <= last_log_index):
                self.become_follower(term)
                self.voted_for = candidate_id
                return True
        return False
    

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.term:
            return False
        self.become_follower(term)
        if self.log[prev_log_index].term != prev_log_term:
            return False
        self.log = self.log[:prev_log_index + 1] + entries
        self.commit_index = min(leader_commit, len(self.log) -1)
        return True


servers = []
server_addr_map = {}
s = zerorpc.Server(RaftServer)
s.run()