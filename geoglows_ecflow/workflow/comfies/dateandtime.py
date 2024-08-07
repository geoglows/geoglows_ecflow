"""
Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import time, datetime
from .py2 import cmp


# -------------------------------------
# Date
# -------------------------------------


class Date(datetime.datetime):

    # alternative constructors

    @classmethod
    def from_ymdh(cls, ymdh):
        return cls.strptime(ymdh, "%Y%m%d%H")
    @classmethod
    def from_ymd(cls, ymd):
        return cls.strptime(ymd, "%Y%m%d")
    @classmethod
    def from_ym(cls, ym):
        return cls.strptime(ym + '01', "%Y%m%d")
    @classmethod
    def from_datetime(cls, datetime):
        return cls(*datetime.timetuple()[:6])

    # to-string conversions

    def __str__(self):
        return self.strftime("%Y%m%d%H")
    @property
    def y(self):
        return self.strftime("%Y")
    @property
    def ym(self):
        return self.strftime("%Y%m")
    @property
    def ymd(self):
        return self.strftime("%Y%m%d")
    @property
    def ymdh(self):
        return self.strftime("%Y%m%d%H")
    @property
    def h(self):
        return self.strftime("%H")
    @property
    def m(self):
        return self.strftime("%m")
    @property
    def hm(self):
        return self.strftime("%H:%M")

    # Need to override these, otherwise date arithmetics
    # returns datetime.datetime object instead of Date object.

    def __add__(self, other):
        result = super(Date, self).__add__(other)
        if isinstance(result, datetime.datetime):
            return Date.from_datetime(result)
        return TimeDelta(result.days, result.seconds)
    def __sub__(self, other):
        result = super(Date, self).__sub__(other)
        if isinstance(result, datetime.datetime):
            return Date.from_datetime(result)
        return TimeDelta(result.days, result.seconds)



# ----------------------------------
# Time
# ----------------------------------


class Time(datetime.time):

    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            raise ValueError(
                    "missing argument(s): expected %H:%M string or integer(s)")
        if isinstance(args[0], str):
            t = time.strptime(args[0], "%H:%M")
            return datetime.time.__new__(cls, t.tm_hour, t.tm_min)
        else:
            return datetime.time.__new__(cls, *args, **kwargs)

    @property
    def ints(self):
        return self.hour, self.minute

    def __str__(self):
        return self.strftime("%H:%M")

    @property
    def hh(self):
        return self.strftime("%H")

    @property
    def mm(self):
        return self.strftime("%M")

    def __add__(self, timedelta):
        dt = datetime.datetime.combine(datetime.date.today(), self)
        dt = dt + timedelta
        t = dt.time()
        return Time(t.hour, t.minute)



# ----------------------------------
# TimeDelta
# ----------------------------------


class TimeDelta(datetime.timedelta):
    pass

HOUR = TimeDelta(hours = 1)
DAY  = HOUR * 24


future = Date.from_ymd('20990101')
past = Date.from_ymd('19000101')


# -----------------------------------
# simple dates generator
# -----------------------------------


def daterange(start, stop, step):
    """
    Like range() but for dates.
    """
    assert(isinstance(start, datetime.datetime))
    assert(isinstance(stop, datetime.datetime))
    assert(isinstance(step, datetime.timedelta))
    direction = cmp(step, TimeDelta(0))
    while cmp(stop, start) == direction:
        yield start
        start += step



# ------------------------------------
# Cron-like dates generator
# ------------------------------------


def calseq(
        start,
        stop,
        times = None,
        months = None,
        monthdays = None,
        weekdays = None
        ):
    assert(isinstance(start, datetime.datetime))
    assert(isinstance(stop, datetime.datetime))
    if start == stop:
        return
    direction = cmp(stop, start)
    curyear = start.replace(month=1, day=1, hour=0)
    startmonth = start.replace(day=1, hour=0)
    startday = start.replace(hour=0)
    if times is None:
        times = [Time(0)]
    if months is None:
        months = range(1,13)
    if monthdays is None:
        monthdays = []
    if weekdays is None:
        weekdays = []
    months = sorted(months, reverse = direction < 0)
    times = sorted(times, reverse = direction < 0)
    while True:
        for month in months:
            if month < 1 or month > 12:
                raise ValueError('month out of range')
            curmonth = curyear.replace(month=month)
            if cmp(startmonth, curmonth) == direction:
                continue
            nmonthdays = [normalize_day(curmonth, d) for d in monthdays]
            if not nmonthdays:
                if not weekdays:
                    nmonthdays = range(1, 32)
            nmonthdays_set = set(nmonthdays)
            if weekdays:
                for day in range(1, 32):
                    try:
                        curday = curmonth.replace(day=day)
                    except ValueError:
                        break
                    if curday.isoweekday() not in weekdays:
                        continue
                    nmonthdays_set.add(day)
            nmonthdays = sorted(list(nmonthdays_set), reverse = (direction < 0))
            for day in nmonthdays:
                try:
                    curday = curmonth.replace(day=day)
                except ValueError:
                    continue
                if cmp(startday, curday) == direction:
                    continue
                for time in times:
                    date = curday.replace(hour=time.hour, minute=time.minute)
                    if cmp(start, date) == direction:
                        continue
                    if cmp(stop, date) != direction:
                        return
                    yield date
        curyear = curyear.replace(year=curyear.year + direction)



class CalSeq(object):

    """
    Implements range, list, next, prev, lsnap, rsnap and shift
    methods equivalent to the 'calseq' command.
    """

    def __init__(
            self,
            weekdays = None,
            monthdays = None,
            months = None,
            hours = None,
            ):
        # the date pattern definition:
        self._hours = hours
        self._months = months
        self._monthdays = monthdays
        self._weekdays = weekdays

    def list(self, start, stop):
        """
        Generate a sequence of dates
        conforming to the pattern.
        (up to but excluding stop date)
        """
        dates = calseq(
                start = start,
                stop = stop,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        return [d for d in dates]

    def range(self, start, stop):
        """
        Generate a sequence of dates
        conforming to the pattern.
        (from start to stop, inclusive)
        """
        if stop > start:
            stop = self.next(stop)
        else:
            stop = self.prev(stop)
        dates = calseq(
                start = start,
                stop = stop,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        return [d for d in dates]

    def lsnap(self, date):
        """
        Normalize date to the closest
        one in the sequence which is
        smaller or equal than date.
        """
        dates = calseq(
                start = date,
                stop = past,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        return next(dates)

    def rsnap(self, date):
        """
        Normalize date to the closest
        one in the sequence which is
        larger or equal than date.
        """
        dates = calseq(
                start = date,
                stop = future,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        return next(dates)

    def next(self, date):
        """
        Return next date from the sequence.
        """
        dates = calseq(
                start = date,
                stop = future,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        d = next(dates)
        if d == date:
            d = next(dates)
        return d

    def prev(self, date):
        """
        Return previous date from the sequence.
        """
        dates = calseq(
                start = date,
                stop = past,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        d = next(dates)
        if d == date:
            d = next(dates)
        return d

    def shift(self, date, n):
        """
        when n>0 - equivalent to calling next() n times.
        when n<0 - equivalent to calling prev() n times.
        wnen n=0 - returns unmodified date.
        """
        if n > 0:
            stop = future
        elif n < 0:
            stop = past
        else:
            return date
        dates = calseq(
                start = date,
                stop = stop,
                weekdays = self._weekdays,
                monthdays = self._monthdays,
                months = self._months,
                times = self._hours
                )
        d = next(dates)
        if d == date:
            d = next(dates)
        for _ in range(1, abs(n)):
            d = next(dates)
        return d



# helper functions for calseq generator

def get_last_day_of_month(date):
    assert(isinstance(date, datetime.datetime))
    for day in (31, 30, 29, 28):
        try:
            date.replace(day=day)
            break
        except ValueError:
            continue
    return day


def normalize_day(date, day):
    """
    Convert negative day number (-1 being last day of
    the month, -2 next to last etc.) to a positive day number.
    """
    if day >= 0:
        return day
    last_day_of_month = get_last_day_of_month(date)
    day = last_day_of_month + day + 1
    if day < 1:
        raise ValueError("negative day {} out of range".format(day))
    return day