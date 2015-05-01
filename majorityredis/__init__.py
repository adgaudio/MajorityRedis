import logging as _logging
log = _logging.getLogger('majorityredis')

from .configure_logging import configure_logging
configure_logging


from .api import MajorityRedis
MajorityRedis
