from math import sqrt
import sys


def avg(l):
    return sum(l) / len(l)


def stddev(l):
    mean = avg(l)
    variance = sum([(n - mean) ** 2 for n in l]) / len(l)
    return sqrt(variance)


def stat(l):
    """Combines avg, stddev, min and max in fewer operations"""
    mean = avg(l)
    minimum = sys.maxint
    maximum = 0.0
    square_sum = 0.0
    for num in l:
        minimum = min(minimum, num)
        maximum = max(maximum, num)
        square_sum += (num - mean) ** 2
    stddev = sqrt(square_sum / len(l))
    return minimum, maximum, mean, stddev
