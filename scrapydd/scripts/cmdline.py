import sys
import scrapydd.main
import scrapydd.scheduleutil
import scrapydd.agent

def main():
    argv = sys.argv
    if len(argv) == 1:
        print 'No command specified.'
        print_commands()
        sys.exit(1)
        return
    cmd = argv[1]
    if cmd == 'agent':
        scrapydd.agent.run(argv)
    elif cmd == 'server':
        scrapydd.main.run(argv)
    elif cmd == 'add_schedule':
        scrapydd.scheduleutil.add_schedule()
    elif cmd == '--help':
        print_commands()
    elif cmd == '--version' or cmd == '-v':
        print_version()
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

def print_version():
    print 'scrapydd v' + scrapydd.__version__

if __name__ == '__main__':
    main()