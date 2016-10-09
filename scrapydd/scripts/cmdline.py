import sys
import scrapydd.main
import scrapydd.executor

def main():
    argv = sys.argv
    cmd = argv.pop(1)
    if cmd == 'agent':
        scrapydd.executor.run(argv)
    elif cmd == 'server':
        scrapydd.main.run(argv)

if __name__ == '__main__':
    main()