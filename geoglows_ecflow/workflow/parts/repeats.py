from geoglows_ecflow.workflow.comfies.ooflow import RepeatEnumerated
from geoglows_ecflow.workflow.comfies.dateandtime import Date, calseq, DAY, CalSeq

_distant_future = Date(2025, 1, 1)

def hindcast_repeat(start, stop=_distant_future):
    """
    Returns ecFlow repeat object.
    Sequence of Monday and Thursday dates.
    """
    dates = calseq(start, stop, weekdays=[1, 4])
    return RepeatEnumerated('YMD', [str(d.ymd) for d in dates])


def equal_hindcast_repeat(start,stop):
    if len(list(calseq(start,stop,weekdays=[4]))) == len(list(calseq(start,stop,weekdays=[1]))):
        dates= calseq(start,stop,weekdays=[1,4])
        return RepeatEnumerated('YMD', [str(d.ymd) for d in dates])
    else:
        stop=stop.replace(stop.year,stop.month,stop.day-1)
        equal_hindcast_repeat(start,stop)

def next_hindcast_repeat(start,stop):
        dates= calseq(start,stop,weekdays=[1,4])
        return dates.next().ymd


def seasonal_repeat(start, stop=_distant_future):
    """
    Sequence of first days of the month.
    """
    dates = calseq(start, stop, monthdays=[1])
    return RepeatEnumerated('YMD', [str(d.ymd) for d in dates])


def calseq_repeat(start, stop=_distant_future,
                  weekdays=[], monthdays=[],
                  months=range(1,13)):
    """
    Sequence of arbitrary dates defined in the same
    way as for comfies.dateandtime.calseq() function
    """
    dates = calseq(start, stop, weekdays=weekdays,
                   monthdays=monthdays, months=months)
    return RepeatEnumerated('YMD', [str(d.ymd) for d in dates])
