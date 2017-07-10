import sys
import time


print(sys.argv)

n = 10
period = 0.1

print('To do: '.format(n))
for i in range(n):
    time.sleep(period)
    print('Worked {} of {} steps'.format(i+1, n))

print('Done')
