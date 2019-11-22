import sys
import time

args = sys.argv

n = 10
if len(args) > 1:
    n = int(args[1])

period = 0.1
if len(args) > 2:
    period = float(args[2])

fail_at = -1
if len(args) > 3:
    fail_at = int(args[3])

print('mkentropy: Running {} steps'.format(n))
for i in range(n):
    if i == fail_at:
        raise RuntimeError('An intended error occurred!')
    time.sleep(period)
    print('mkentropy: Did {} of {} steps: {}%'.format(i + 1, n, (100 * (i + 1)) / n))

print('mkentropy: Done making some entropy')
