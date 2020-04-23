import os
from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing
import threading
import time

class CommandParameter(object):
    def set(self, commandLine, timeout=60, retry=3, backoff=0, delay=0):
        self.commandLine = commandLine
        self.timeout = timeout
        self.retry = retry
        self.delay = delay
        self.backoff = backoff

        self.validate()

        return self

    def setFromStrings(self, strings, separator=";"):
        commandLine, timeout, retry, backoff, delay = (strings.strip() + ";;;;").split(separator)[0:5]
        return self.set(commandLine, int(timeout or "60"), int(retry or "3"), int(backoff or "0"), int(delay or "0"))

    def validate(self):
        if not self.commandLine:
            raise ValueError("Command Line is blank.")

        if self.timeout < 1:
            raise ValueError("Set timeout value to greater than or equals 1.")

        if self.retry < 1:
            raise ValueError("Set number of retries to greater than or equals 1.")

        if self.delay < 0:
            raise ValueError("Set delay to positive value.")

        if self.backoff < 0:
            raise ValueError("Set backoff to positive value.")

class CommandLineExecutor(object):
    def execute(self, parameter):
        for trialCounter in range(1, parameter.retry + 1):
            try:
                process = subprocess.Popen(parameter.commandLine, shell=True, text=True)
                process.communicate(timeout=parameter.timeout)
            except subprocess.TimeoutExpired:
                print(f"Timed out({trialCounter}/{parameter.retry}): {parameter.commandLine}")
                process.terminate()

                # Exponential backoff
                time.sleep(parameter.backoff ** trialCounter + parameter.delay)
            else:
                return process.returncode

        return -1

    def executeFromStrings(self, strings):
        return self.execute(CommandParameter().setFromStrings(strings))

class CommandListReader(object):
    def readFromFile(self, filename, encoding='shift-jis'):
        commandParameters = []

        with open(filename, mode='r', encoding=encoding) as f:
            return self.readFromStringList(f.readlines())

        return commandParameters

    def readFromStringList(self, stringList):
        commandParameters = []

        for string in stringList:
            commandParameters.append(CommandParameter().setFromStrings(string))

        return commandParameters

class __CommandListExecutor(CommandLineExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self._allResults = { 0: 0 }
        self._allResultsDetails = {}
        self.lock = threading.Lock()

    @property
    def allResultsDetails(self):
        return self._allResultsDetails

    @property
    def allResults(self):
        return self._allResults

    def execute(self, parameter):
        returnCode = super().execute(parameter)
        commandName = os.path.basename(parameter.commandLine.split(" ")[0])
        self.lock.acquire()
        self._allResultsDetails[commandName] = returnCode
        self.lock.release()

        return returnCode

    def executeFromFile(self, filename):
        return self.execute(CommandListReader().readFromFile(filename))

    def executeFromStringList(self, stringList):
        return self.execute(CommandListReader().readFromStringList(stringList))

    def __sortDictionary(self, dictionary, sortByKey=True, reverse=False):
        return sorted(dictionary.items(), key=lambda item:item[0 if sortByKey == True else 1], reverse=reverse)

    def _incrementDictionary(self, dictionary, key):
        if key not in dictionary:
            dictionary[key] = 1
        else:
            dictionary[key] = dictionary[key] + 1

    def _tallyingAllResults(self, allResults):
        self._allResults = self.__sortDictionary(allResults)

        if allResults[0] == self.numOfcommandParameters:
            return 0

        del allResults[0]

        # If key value has non zero value , sort a dict by item value(this is counter).
        sortedByValueDesc = self.__sortDictionary(allResults, sortByKey=False, reverse=True)
        topOfCount = next(iter(sortedByValueDesc))[1]

        # If item value is not unique, pick up more highly key value.
        sortedByKeyDesc = self.__sortDictionary(allResults, reverse=True)
        for key, value in sortedByKeyDesc:
            if value == topOfCount:
                return key

class CommandListParallelExecutor(__CommandListExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self._allResults = { 0: 0 }
        self._allResultsDetails = {}
        self.lock = threading.Lock()

    def execute(self, commandParameters):
        if not commandParameters:
            raise ValueError("Command parameters not set.")

        self.numOfcommandParameters = len(commandParameters)
        self._allResults = { 0: 0 }

        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            allFutureResults = list(executor.map(super().execute, commandParameters))
            executor.shutdown()

        self._allResults = { 0: 0 }
        for futureResults in list(allFutureResults):
            self._incrementDictionary(self._allResults, futureResults)

        return self._tallyingAllResults(self._allResults)

class CommandListSerialExecutor(__CommandListExecutor):
    def __init__(self):
        self.numOfcommandParameters = 0
        self._allResults = { 0: 0 }
        self._allResultsDetails = {}
        self.hasSetThreshouldOfError = False
        self._thresholdOfError = 0
        self.lock = threading.Lock()

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

        self.numOfcommandParameters = len(commandParameters)

        self._allResults = { 0: 0 }
        for index, parameter in enumerate(commandParameters):
            returnCode = super().execute(parameter)
            self._incrementDictionary(self._allResults, returnCode)
            if self.__isDoStopAtError(returnCode):
                print(f"Stop at {index + 1}, return code: {returnCode}, threshold: {self._thresholdOfError}.")
                break

        return self._tallyingAllResults(self._allResults)

    def __isDoStopAtError(self, returnCode):
        if self.hasSetThreshouldOfError == False:
            return False
        
        return True if returnCode >= self._thresholdOfError or returnCode == -1 else False

if __name__ == "__main__":
    # Code exsamples
    parameters = [
        CommandParameter().set("echo hoge", 10, 3),
        CommandParameter().setFromStrings("timeout /t 3 /nobreak > nul;1;5")
    ]

    # Execute command line single.
    print("--- Execute command line single. ---")
    finalResults = CommandLineExecutor().execute(parameters[0])
    print(finalResults)

    # Execute command list serial.
    print("--- Execute command list serial. ---")
    finalResults = CommandListSerialExecutor().execute(parameters)
    print(finalResults)

    # If you set threshold of error, you can been break a command line at error occur or timed out.
    print("--- Execute command list serial with thresold. ---")
    executor = CommandListSerialExecutor()
    executor.thresholdOfError = 5
    finalResults = executor.execute(parameters)
    print(finalResults)

    # Execute command list parallel.
    print("--- Execute command list parallel. ---")
    finalResults = CommandListParallelExecutor().execute(parameters)
    print(finalResults)

    # You can execute command list from file.
    filename = os.path.join("c:\\temp", "commandList.txt")

    print("--- Execute command list serial from file. ---")
    finalResults = CommandListSerialExecutor().executeFromFile(filename)
    print(finalResults)

    print("--- Execute command list parallel from file. ---")
    finalResults = CommandListParallelExecutor().executeFromFile(filename)
    print(finalResults)

    # You can execute command list from string list
    parameterList = [
        "echo hoge",
        "timeout /t 3 /nobreak > nul;1;5"
    ]

    print("--- Execute command line from strings. ---")
    finalResults = CommandLineExecutor().executeFromStrings(parameterList[0])
    print(finalResults)

    print("--- Execute command list serial from string list. ---")
    finalResults = CommandListSerialExecutor().executeFromStringList(parameterList)
    print(finalResults)

    print("--- Execute command list parallel from string list. ---")
    finalResults = CommandListParallelExecutor().executeFromStringList(parameterList)
    print(finalResults)

    # Execute command list serial.
    print("--- get all results summaries and details after execute command list serial. ---")
    clse = CommandListSerialExecutor()
    finalResults = clse.execute(parameters)
    print(finalResults)
    print(clse.allResults)
    print(clse.allResultsDetails)

    # Execute command list parallel.
    print("--- get all results summaries and details after execute command list parallel. ---")
    clpe = CommandListParallelExecutor()
    finalResults = clpe.execute(parameters)
    print(finalResults)
    print(clpe.allResults)
    print(clpe.allResultsDetails)


    exit(0)