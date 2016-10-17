import sys
import scrapydd.main
import scrapydd.executor
import scrapydd.scheduleutil

def main():
    argv = sys.argv
    if len(argv) == 1:
        print 'No command specified.'
        print_commands()
        sys.exit(1)
        return
    cmd = argv[1]
    if cmd == 'agent':
        scrapydd.executor.run(argv)
    elif cmd == 'server':
        scrapydd.main.run(argv)
    elif cmd == 'add_schedule':
        scrapydd.scheduleutil.add_schedule()
    elif cmd == '--help':
        print_commands()
    else:
        print 'Invalid command.'
        print_commands()

def print_commands():
    print 'usage: scrapydd {command} [options]'
    print 'Available commands:'
    print '\tserver:\trun scrapydd server.'
    print '\tagent:\trun scrapydd agent.'
    print '\tadd_schedule:\tadd a schedule to spider.'
    print ''
    print 'use scrapydd {command} --help for further help.'
if __name__ == '__main__':
    main()