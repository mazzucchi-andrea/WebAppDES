# --------------------------------------------------------------------------
# This is an ANSI C library for generating random variates from six discrete
# distributions
#
#     Generator         Range (x)     Mean         Variance
#
#     Bernoulli(p)      x = 0,1       p            p*(1-p)
#     Binomial(n, p)    x = 0,...,n   n*p          n*p*(1-p)
#     Equilikely(a, b)  x = a,...,b   (a+b)/2      ((b-a+1)*(b-a+1)-1)/12
#     Geometric(p)      x = 0,...     p/(1-p)      p/((1-p)*(1-p))
#     Pascal(n, p)      x = 0,...     n*p/(1-p)    n*p/((1-p)*(1-p))
#     Poisson(m)        x = 0,...     m            m
#
# and seven continuous distributions
#
#     Uniform(a, b)     a < x < b     (a + b)/2    (b - a)*(b - a)/12
#     Exponential(m)    x > 0         m            m*m
#     Erlang(n, b)      x > 0         n*b          n*b*b
#     Normal(m, s)      all x         m            s*s
#     Lognormal(a, b)   x > 0            see below
#     Chisquare(n)      x > 0         n            2*n
#     Student(n)        all x         0  (n > 1)   n/(n - 2)   (n > 2)
#
# For the a Lognormal(a, b) random variable, the mean and variance are
#
#                       mean = exp(a + 0.5*b*b)
#                   variance = (exp(b*b) - 1) #exp(2*a + b*b)
#
# Name              : rvgs.c  (Random Variate GeneratorS)
# Author            : Steve Park & Dave Geyer
# Language          : ANSI C
# Latest Revision   : 10-28-98
# Translated by     : Philip Steele
# Language          : Python 3.3
# Latest Revision   : 3/26/14
#
# --------------------------------------------------------------------------

from math import log, sqrt, exp

from rngs import random


def bernoulli(p):
    # ========================================================
    # Returns 1 with probability p or 0 with probability 1 - p.
    # NOTE: use 0.0 < p < 1.0
    # ========================================================

    if random() < 1 - p:
        return 0
    else:
        return 1


def binomial(n, p):
    # ================================================================
    # Returns a binomial distributed integer between 0 and n inclusive.
    # NOTE: use n > 0 and 0.0 < p < 1.0
    # ================================================================

    x = 0

    for _ in range(0, n):
        x += bernoulli(p)
    return x


def equilikely(a, b):
    # ===================================================================
    # Returns an equilikely distributed integer between a and b inclusive.
    # NOTE: use a < b
    # ===================================================================
    return a + int((b - a + 1) * random())


def geometric(p):
    # ====================================================
    # Returns a geometric distributed non-negative integer.
    # NOTE: use 0.0 < p < 1.0
    # ====================================================
    #

    return int(log(1.0 - random()) / log(p))


def pascal(n, p):
    # =================================================
    # Returns a Pascal distributed non-negative integer.
    # NOTE: use n > 0 and 0.0 < p < 1.0
    # =================================================
    #

    x = 0

    for _ in range(0, n):
        x += geometric(p)
    return x


def poisson(m):
    # ==================================================
    # Returns a Poisson distributed non-negative integer.
    # NOTE: use m > 0
    # ==================================================
    #
    t = 0.0
    x = 0

    while t < m:
        t += exponential(1.0)
        x += 1

    return x - 1


def uniform(a, b):
    # ===========================================================
    # Returns a uniformly distributed real number between a and b.
    # NOTE: use a < b
    # ===========================================================
    #
    return a + (b - a) * random()


def exponential(m):
    # =========================================================
    # Returns an exponentially distributed positive real number.
    # NOTE: use m > 0.0
    # =========================================================
    #
    return -m * log(1.0 - random())


def erlang(n, b):
    # ==================================================
    # Returns an Erlang distributed positive real number.
    # NOTE: use n > 0 and b > 0.0
    # ==================================================
    #
    x = 0.0

    for _ in range(0, n):
        x += exponential(b)
    return x


def normal(m, s):
    # ========================================================================
    # Returns a normal (Gaussian) distributed real number.
    # NOTE: use s > 0.0
    #
    # Uses a very accurate approximation of the normal idf due to Odeh & Evans,
    # J. Applied Statistics, 1974, vol 23, pp 96-97.
    # ========================================================================
    #
    p0 = 0.322232431088
    q0 = 0.099348462606
    p1 = 1.0
    q1 = 0.588581570495
    p2 = 0.342242088547
    q2 = 0.531103462366
    p3 = 0.204231210245e-1
    q3 = 0.103537752850
    p4 = 0.453642210148e-4
    q4 = 0.385607006340e-2

    u = random()
    if u < 0.5:
        t = sqrt(-2.0 * log(u))
    else:
        t = sqrt(-2.0 * log(1.0 - u))

    p = p0 + t * (p1 + t * (p2 + t * (p3 + t * p4)))
    q = q0 + t * (q1 + t * (q2 + t * (q3 + t * q4)))

    if u < 0.5:
        z = (p / q) - t
    else:
        z = t - (p / q)

    return m + s * z


def lognormal(a, b):
    # ====================================================
    # Returns a lognormal distributed positive real number.
    # NOTE: use b > 0.0
    # ====================================================
    #
    return exp(a + b * normal(0.0, 1.0))


def chisquare(n):
    # =====================================================
    # Returns a chi-square distributed positive real number.
    # NOTE: use n > 0
    # =====================================================
    #
    x = 0.0

    for _ in range(0, n):
        z = normal(0.0, 1.0)
        x += z * z

    return x


def student(n):
    # ===========================================
    # Returns a student-t distributed real number.
    # NOTE: use n > 0
    # ===========================================
    #
    return normal(0.0, 1.0) / sqrt(chisquare(n) / n)


def test_functions():
    # tests to ensure that all variates match what was produced by C version of program (with the same order and parameters)

    # bernoulli
    bern = []
    for _ in range(0, 10):
        bern.append(bernoulli(.65))

    if bern == [0, 0, 1, 0, 1, 0, 1, 1, 1, 1]:
        print("Bernoulli test passed")
    else:
        print("FIX BERNOULLI!")

    # binomial
    bino = binomial(50, .19)
    if bino == 7:
        print("Binomial test passed")
    else:
        print("FIX BINOMIAL")
        print(bino)

    # equilikely
    equi = equilikely(32, 108)
    if equi == 77:
        print("Equilikely test passed")
    else:
        print("FIX EQUILIKELY")

    # geo test
    geo = []
    for _ in range(0, 10):
        geo.append(geometric(.93))

    if geo == [0, 8, 4, 21, 7, 3, 17, 5, 14, 1]:
        print("Geo test passed")
    else:
        print("Fix geo!")
        print(geo)

    # pascal test
    pas = pascal(87, .93)
    if pas == 1407:
        print("Pascal test passed!")
    else:
        print("FIX PASCAL")

    # poisson test
    pois = poisson(13.3)
    if pois == 15:
        print("Poisson test passed!")
    else:
        print("FIX POISSON!")

    # uniform test
    uni = uniform(32, 108)
    if round(uni, 6) == 78.976127:
        print("Uniform test passed!")
    else:
        print("FIX UNIFORM. Produced: ", uni)
        print("Expected: 78.976127")

    # exp test
    ret = exponential(4.7)
    if round(ret, 6) == 4.796404:
        print("Exponential test passed!")
    else:
        print("FIX EXP. Produced: ", ret)
        print("expected: 4.796404")

    # erlang test
    erl = erlang(41, .08)
    if round(erl, 6) == 2.824873:
        print("Erland test passed!")
    else:
        print("FIX ERLANG Produced: ", erl)
        print("Expected: 2.824873")

    # normal test
    norm = normal(8.9, 4)
    if round(norm, 6) == 8.374243:
        print("Normal test passed!")
    else:
        print("FIX NORMAL Produced: ", norm)
        print("Expected: 8.374243")

    # lognormal test
    lnorm = lognormal(1.31, 1.6)
    if round(lnorm, 6) == 5.533064:
        print("Lognormal test passed!")
    else:
        print("FIX LOGNORMAL Produced: ", lnorm)
        print("Expected: 5.533064")

    # chisq test
    chisq = chisquare(39)
    if round(chisq, 6) == 33.634524:
        print("Chisquare test passed!")
    else:
        print("FIX CHI-SQUARE - Produced: ", chisq)
        print("Expected: 33.634524")

    # t test
    stu = student(61)
    if round(stu, 6) == -1.429058:
        print("Student test passed!")
    else:
        print("FIX STUDENT - Produced: ", stu)
        print("Expected: -1.429058")
