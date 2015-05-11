class MajorityRedisException(Exception):
    pass


class CannotObtainLock(MajorityRedisException):
    pass


class ConsumeError(MajorityRedisException):
    pass


class NoMajority(MajorityRedisException):
    pass


class TooManyRetries(MajorityRedisException):
    pass


class Timeout(MajorityRedisException):
    pass
