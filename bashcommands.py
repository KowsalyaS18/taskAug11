import schedule
import time
import os
def call():
	os. system('sh sample.sh')

schedule.every(2).minutes.do(call)
while True:
    schedule.run_pending()
    time.sleep(1)
