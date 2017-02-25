import queue


class QueuePipe(object):
    def __init__(self):
        self.request_match = queue.Queue()
        self.request_matchlist = queue.Queue()
        self.flush_match = queue.Queue()
        self.flush_matchlist = queue.Queue()
        self.dispatch_match_flush = queue.Queue()
        self.dispatch_matchlist_flush = queue.Queue()
        self.dispatch_matchlist_discover = queue.Queue()
