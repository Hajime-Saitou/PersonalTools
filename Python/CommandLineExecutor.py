import os
from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing
import threading
import time
import json

class CommandParameter(object):
    defaultTimeout = 60
    defaultRetry = 3
    defaultBakcoff = 0
    defaultDelay = 0

    def __init__(self):
        self.parameterCount = 5
        self.separator = ";"

    @property
    def separator(self):
        return self._separator

    @separator.setter
    def separator(self, separator):
        self._separator = separator
        self.repeatedSeparator = ""
        for _ in range(self.parameterCount - 1):
            self.repeatedSeparator += self._separator

    def set(self, commandLine, timeout = None, retry = None, backoff = None, delay = None):
        self.commandLine = commandLine
        self.timeout = int(timeout) if timeout is not None else CommandParameter.defaultTimeout
        self.retry = int(retry) if retry is not None else CommandParameter.defaultRetry
        self.delay = int(delay) if delay is not None else CommandParameter.defaultBakcoff
        self.backoff = int(backoff) if backoff is not None else CommandParameter.defaultDelay

        self.validate()

        return self

    def setFromString(self, string):
        commandLine, timeout, retry, backoff, delay = (string.strip() + self.repeatedSeparator).split(self._separator)[0:self.parameterCount]
        return self.set(commandLine, timeout or None, retry or None, backoff or None, delay or None)

    def getParameterStringFromJsonValue(self, jsonValue, childKey):
        return jsonValue[childKey] if childKey in jsonValue else None

    def setFromJsonValue(self, jsonValue):
        commandLine = self.getParameterStringFromJsonValue(jsonValue, "commandLine")
        timeout = self.getParameterStringFromJsonValue(jsonValue, "timeout")
        retry = self.getParameterStringFromJsonValue(jsonValue, "retry")
        backoff = self.getParameterStringFromJsonValue(jsonValue, "backoff")
        delay = self.getParameterStringFromJsonValue(jsonValue, "delay")
        return CommandParameter().set(commandLine, timeout, retry, backoff, delay)

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

    def toString(self):
        return self.separator.join([self.commandLine, str(self.timeout), str(self.retry), str(self.backoff), str(self.delay)])

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

    def executeFromString(self, string):
        return self.execute(CommandParameter().setFromString(string))

class CommandListReader(object):
    def readFromFile(self, filename, encoding='shift-jis'):
        with open(filename, mode='r', encoding=encoding) as f:
            return self.readFromStringList(f.readlines())

    def readFromStringList(self, stringList):
        commandParameters = []

        for string in stringList:
            commandParameters.append(CommandParameter().setFromString(string))

        return commandParameters

class ResultsCollector(object):
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
        return dict(self.__sortDictionary(self._allResults))

    def clearResults(self):
        self.numOfcommandParameters = 0
        self._allResults = { 0: 0 }
        self._allResultsDetails = {}

    def collectResults(self, commandLine, returnCode):
        self.lock.acquire()
        self.numOfcommandParameters += 1
        self._incrementDictionary(self._allResults, returnCode)
        commandName = os.path.basename(commandLine.split()[0])
        self._allResultsDetails[commandName] = returnCode
        self.lock.release()

    def __sortDictionary(self, dictionary, orderByKey=True, desending=False):
        return sorted(dictionary.items(), key=lambda item:item[0 if orderByKey == True else 1], reverse=desending)

    def _incrementDictionary(self, dictionary, key):
        if key not in dictionary:
            dictionary[key] = 1
        else:
            dictionary[key] = dictionary[key] + 1

    def tallyingAllResults(self):
        if self._allResults[0] == self.numOfcommandParameters:
            return 0

        allResultsWithoutZero = self._allResults.copy()
        # delete key 0 always
        del allResultsWithoutZero[0]

        if self._allResults[0] == 0:
            del self._allResults[0]

        # If key value has non zero value , sort a dict by item value(this is counter).
        sortedByValueDesc = self.__sortDictionary(allResultsWithoutZero, orderByKey=False, desending=True)
        topOfCount = next(iter(sortedByValueDesc))[1]

        # If item value is not unique, pick up more highly key value.
        sortedByKeyDesc = self.__sortDictionary(allResultsWithoutZero, desending=True)
        for key, value in sortedByKeyDesc:
            if value == topOfCount:
                return key

class __CommandListExecutor(CommandLineExecutor, ResultsCollector):
    def __init__(self):
        self.clearResults()
        self.lock = threading.Lock()

    def execute(self, parameter):
        returnCode = super().execute(parameter)
        self.collectResults(parameter.commandLine, returnCode)

        return returnCode

    def executeFromFile(self, filename):
        return self.execute(CommandListReader().readFromFile(filename))

    def executeFromStringList(self, stringList):
        return self.execute(CommandListReader().readFromStringList(stringList))

class CommandListParallelExecutor(__CommandListExecutor, ResultsCollector):
    def __init__(self):
        self.clearResults()
        self.lock = threading.Lock()

    def execute(self, commandParameters):
        if not commandParameters:
            raise ValueError("Command parameters not set.")

        self.clearResults()
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            list(executor.map(super().execute, commandParameters))
            executor.shutdown()

        return self.tallyingAllResults()

class CommandListSerialExecutor(__CommandListExecutor):
    def __init__(self):
        super().__init__()
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

        super().clearResults()
        for index, parameter in enumerate(commandParameters):
            returnCode = super().execute(parameter)
            if self.__isDoStopAtError(returnCode):
                print(f"Stop at {index + 1}, return code: {returnCode}, threshold: {self._thresholdOfError}.")
                break

        return self.tallyingAllResults()

    def __isDoStopAtError(self, returnCode):
        if self.hasSetThreshouldOfError == False:
            return False
        
        return True if returnCode >= self._thresholdOfError or returnCode == -1 else False

class BatchExecutor(ResultsCollector):
    def __init__(self):
        super().__init__()
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

    def __isDoStopAtError(self, returnCode):
        if self.hasSetThreshouldOfError == False:
            return False
        
        return True if returnCode >= self._thresholdOfError or returnCode == -1 else False

    def executeFromShallowJson(self, filename):
        with open(filename) as file:
            jsonCommandList = json.load(file)

            self.clearResults()

            for jsonItem in jsonCommandList.items():
                returnCode = self.__executeFromJsonItem(jsonItem)
                if self.__isDoStopAtError(returnCode):
                    print(f"Stop at {jsonItem[0]}, return code: {returnCode}, threshold: {self._thresholdOfError}.")
                    break

            return self.tallyingAllResults()

    def __executeFromJsonItem(self, jsonItem):
        jsonKey, jsonValue = jsonItem
        if not type(jsonValue) is list:
            returnCode = CommandLineExecutor().execute(CommandParameter().setFromJsonValue(jsonValue))
        else:
            commandList = []
            for value in jsonValue:
                commandList.append(CommandParameter().setFromJsonValue(value))

            returnCode = CommandListParallelExecutor().execute(commandList)
        self.collectResults(jsonKey, returnCode)

        return returnCode

if __name__ == "__main__":
    # Code exsamples
    parameters = [
        CommandParameter().set("echo hoge", 10, 3),
        CommandParameter().setFromString("timeout /t 3 /nobreak > nul;1;5")
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

    print("--- Execute command line from string. ---")
    finalResults = CommandLineExecutor().executeFromString(parameterList[0])
    print(finalResults)

    print("--- Execute command list serial from string list. ---")
    finalResults = CommandListSerialExecutor().executeFromStringList(parameterList)
    print(finalResults)

    print("--- Execute command list parallel from string list. ---")
    finalResults = CommandListParallelExecutor().executeFromStringList(parameterList)
    print(finalResults)

    # Execute command list serial.
    print("--- Get all results summaries and details after execute command list serial. ---")
    clse = CommandListSerialExecutor()
    finalResults = clse.execute(parameters)
    print(finalResults)
    print(clse.allResults)
    print(clse.allResultsDetails)

    # Execute command list parallel.
    print("--- Get all results summaries and details after execute command list parallel. ---")
    clpe = CommandListParallelExecutor()
    finalResults = clpe.execute(parameters)
    print(finalResults)
    print(clpe.allResults)
    print(clpe.allResultsDetails)

    # You can execute batch job from shallow JSON.
    filename = os.path.join("c:\\temp", "commandList.json")

    print("--- Execute batch job from shallow JSON. ---")
    be = BatchExecutor()
    finalResults = be.executeFromShallowJson(filename)
    print(finalResults)
    print(be.allResults)
    print(be.allResultsDetails)

    exit(0)