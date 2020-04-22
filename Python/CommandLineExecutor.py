import os
from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing

class CommandParameter(object):
    def __init__(self, commandLine, timeout, retry):
        if not commandLine:
            raise ValueError("Command Line is blank.")

        if timeout < 1:
            raise ValueError("Set timeout value to greater than or equals 1.")

        if retry < 1:
            raise ValueError("Set number of retries to greater than or equals 1.")

        self.commandLine = commandLine
        self.timeout = timeout
        self.retry = retry

class CommandLineExecutor(object):
    def execute(self, parameter):
        for trialCounter in range(parameter.retry):
            try:
                process = subprocess.Popen(parameter.commandLine, shell=True, text=True)
                process.communicate(timeout=parameter.timeout)
            except subprocess.TimeoutExpired:
                print(f"Timed out({trialCounter + 1}/{parameter.retry}): {parameter.commandLine}")
                process.terminate()
            else:
                return process.returncode

        return -1

class CommandListReader(object):
    def __init__(self, separator = ";"):
        self.separator = separator

    def read(self, filename, encoding='shift-jis'):
        commandParameters = []

        with open(filename, mode='r', encoding=encoding) as f:
            for lineNo, readLine in enumerate(f.readlines()):
                readLine = readLine.strip()
                if not readLine:
                    print(f"Line {lineNo + 1}: Command line is blank.")
                    continue

                commandLine, timeout, retry = readLine.split(self.separator)
                commandParameters.append(CommandParameter(commandLine, int(timeout), int(retry)))

        return commandParameters

class __CommandListExecutor(CommandLineExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self.allResults = {0: 0}

    def _incrementAllResults(self,  key):
        if key not in self.allResults:
            self.allResults[key] = 1
        else:
            self.allResults[key] = self.allResults[key] + 1

    def executeFromFile(self, filename):
        return self.execute(CommandListReader().read(filename))

    def __sortDictionary(self, dictionary, sortByKey=True, reverse=False):
        return sorted(dictionary.items(), key=lambda item:item[0 if sortByKey == True else 1], reverse=reverse)

    def _tallyingAllResults(self):
        sortedByKeyAsc = self.__sortDictionary(self.allResults)
        print(f"All results: {sortedByKeyAsc}")

        if self.allResults[0] == self.numOfcommandParameters:
            return 0

        del self.allResults[0]

        # If key value has non zero value , sort a dict by item value(this is counter).
        sortedByValueDesc = self.__sortDictionary(self.allResults, sortByKey=False, reverse=True)
        topOfCount = next(iter(sortedByValueDesc))[1]

        # If item value is not unique, pick up more highly key value.
        sortedByKeyDesc = self.__sortDictionary(self.allResults, reverse=True)
        for key, value in sortedByKeyDesc:
            if value == topOfCount:
                return key

class CommandListParallelExecutor(__CommandListExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self.allResults = {0: 0}

    def execute(self, commandParameters):
        if not commandParameters:
            raise ValueError("Command parameters not set.")

        self._numOfcommandParameters = len(commandParameters)

        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            allFutureResults = list(executor.map(super().execute, commandParameters))
            executor.shutdown()

        for futureResults in list(allFutureResults):
            self._incrementAllResults(futureResults)

        return self._tallyingAllResults()

class CommandListSerialExecutor(__CommandListExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self.allResults = {0: 0}
        self.hasSetThreshouldOfError = False
        self._thresholdOfError = 0

    @property
    def thresholdOfError(self, value):
        return self._thresholdOfError

    @thresholdOfError.setter
    def thresholdOfError(self, value):
        if value < 0:
            raise ValueError("Set threshold to greater than or equals 0.")

        self.hasSetThreshouldOfError = True
        self._thresholdOfError = value

    def execute(self, commandParameters):
        if not commandParameters:
            raise ValueError("Command parameters not set.")

        self._numOfcommandParameters = len(commandParameters)

        for index, parameter in enumerate(commandParameters):
            returnCode = super().execute(parameter)
            self._incrementAllResults(returnCode)
            if self.__isDoStopAtError(returnCode):
                print(f"Stop at {index + 1}, return code: {returnCode}, threshold: {self._thresholdOfError}.")
                break

        return self._tallyingAllResults()

    def __isDoStopAtError(self, returnCode):
        if self.hasSetThreshouldOfError == False:
            return False
        
        return True if returnCode >= self._thresholdOfError or returnCode == -1 else False

if __name__ == "__main__":
    # Code exsamples
    parameters = [
        CommandParameter("echo hoge", 10, 3),
        CommandParameter("timeout /t 3 /nobreak > nul", 1, 5)
    ]

    # Execute command line single.
    print("Execute command line single.")
    finalResults = CommandLineExecutor().execute(parameters[0])
    print(finalResults)

    # Execute command list serial.
    print("Execute command list serial.")
    finalResults = CommandListSerialExecutor().execute(parameters)
    print(finalResults)

    # If you set threshold of error, you can been break a command line at error occur or timed out.
    print("Execute command list serial with thresold.")
    executor = CommandListSerialExecutor()
    executor.thresholdOfError = 5
    finalResults = executor.execute(parameters)
    print(finalResults)

    # Execute command list parallel.
    print("Execute command list parallel.")
    finalResults = CommandListParallelExecutor().execute(parameters)
    print(finalResults)

    # You can execute command list from file.
    filename = os.path.join("c:\\temp", "commandList.txt")

    print("Execute command list serial from file.")
    finalResults = CommandListSerialExecutor().executeFromFile(filename)
    print(finalResults)

    print("Execute command list parallel from file.")
    finalResults = CommandListParallelExecutor().executeFromFile(filename)
    print(finalResults)

    # Redundant way of writing.
    parameters = CommandListReader().read(filename)

    print("Execute command list serial from file(redundant).")
    finalResults = CommandListSerialExecutor().execute(parameters)
    print(finalResults)

    print("Execute command list parallel from file(redundant).")
    finalResults = CommandListParallelExecutor().execute(parameters)
    print(finalResults)

    exit(0)