import logging as _logging
log = _logging.getLogger('majorityredis')

from .configure_logging import configure_logging
configure_logging(True, log)

from .util import retry_condition
retry_condition

from .api import MajorityRedis
MajorityRedis
