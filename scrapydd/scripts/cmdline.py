import sys
import scrapydd.main
import scrapydd.executor
import scrapydd.scheduleutil

def main():
    argv = sys.argv
    cmd = argv.pop(1)
    if cmd == 'agent':
        scrapydd.executor.run(argv)
    elif cmd == 'server':
        scrapydd.main.run(argv)
    elif cmd == 'add_schedule':
        scrapydd.scheduleutil.add_schedule()

if __name__ == '__main__':
    main()