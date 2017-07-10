import sys
import time

args = sys.argv

n = 10
if len(args) > 1:
    n = int(args[1])

period = 0.1
if len(args) > 2:
    period = float(args[2])

print('mkentropy: Running {} steps'.format(n))
for i in range(n):
    time.sleep(period)
    print('mkentropy: Did {} of {} steps: {}%'.format(i+1, n, (100*(i+1))/n))

print('mkentropy: Done making some entropy')
