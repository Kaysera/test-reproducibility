#!/usr/bin/python3
from time import sleep
import random
import sys
import os
sleep(15)
x = sys.argv[1]
x = os.environ['PBS_ARRAY_INDEX']
fn = random.randrange(0,50)
print('Holo from %s' % x)
print('Alo from %s' % x, file=sys.stderr)
with open('./potato-%s.txt' % x, 'w+') as f:
	f.write('Buenas tardes')
