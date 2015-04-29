from .lockingqueue import LockingQueue as _LockingQueue
from .lock import Lock as _Lock
from .getset import GetSet as _GetSet
from .majorityredis_base import MajorityRedisBaseClass


class MajorityRedis(MajorityRedisBaseClass):
    def __init__(self, *args, **kwargs):
        """Initializes MajorityRedis connection to multiple independent
        non-replicated Redis Instances.  This MajorityRedis client contains
        algorithms and operations based on majority vote of the redis servers.

        Please initialize it by passing the following parameters:
        """
        super(MajorityRedis, self).__init__(*args, **kwargs)
        self._args, self._kwargs = args, kwargs
        self.Lock = _Lock(*args, **kwargs)
        getset = _GetSet(*args, **kwargs)
        self.get = getset.get
        self.set = getset.set

    __init__.__doc__ += MajorityRedisBaseClass.__init__.__doc__

    def LockingQueue(self, queue_path):
        return _LockingQueue(queue_path, *self._args, **self._kwargs)
    LockingQueue.__doc__ == _LockingQueue.__doc__
