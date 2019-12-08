from tm.TransManager import TransactionManager
import argparse
import itertools as it 
import os
import sys
import re

def commandParser(line):
    """
    parse the command into a tuple (operator, [args..])
    Author: Xinsen Lu
    input: line of the command
    output: list of parsed command
    side effect: None
    """
    # ignore comments
    if "//" in line:
        return None
    commandList = []
    for command in line.split(";"):
        # In case stdin input, it is better to have delimiter to identify different groups
        match = re.match("([a-zA-Z0-9]+)\(([^)]*)\)", command.strip())
        if match is not None:
            operation, args = match.groups()
            operation = operation.strip()
            argslist = args.split(",")
            argslist = [item.strip() for item in argslist]
            commandList.append((operation, argslist))
    return commandList

	
class fileReader():
    """
    file reader
    Author: Xinsen Lu
    input: file path
    output: command iterator
    side effect: None
    """
    def __init__(self, file):
        self._file = file
        self._command = []
        self.parse()
    def parse(self):
        with open(self._file, "r") as f:
            for _, line in enumerate(f):
                command = commandParser(line)
                if command is not None:
                    self._command.extend(command)
    
    def __iter__(self):
        return iter(self._command)



class streamReader():
    """
    stream reader
    Author: Xinsen Lu
    input: None
    output: command iterator
    side effect: None
    """
    def __init__(self, stream):
        self._stream = stream

    def __iter__(self):
        for line in self._stream:
            commands = commandParser(line)
            if len(commands) > 0:
                for command in commands:
                    print(command)
                    yield command





def main():
    description = "Advanced Database"   
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-f', '--file',
			dest='test_file',
			help='Path to file.')
    args = parser.parse_args()
    tm = TransactionManager()
    if args.test_file is None:
        commandIter = streamReader(sys.stdin)
    else:
        commandIter = fileReader(args.test_file)
    tm.loadCommand(commandIter)


if __name__=='__main__':
    main()



