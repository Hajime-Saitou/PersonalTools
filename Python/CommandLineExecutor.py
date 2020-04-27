import os
from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing
import threading
import time
import json

class CommandParameter(object):
    def __init__(self):
        self.separatorRepeatCount = 4
        self.separator = ";"

    @property
    def separator(self):
        return self._separator

    @separator.setter
    def separator(self, separator):
        self._separator = separator
        self.repeatedSeparator = ""
        for _ in range(self.separatorRepeatCount):
            self.repeatedSeparator += self._separator

    def set(self, commandLine, timeout=60, retry=3, backoff=0, delay=0):
        self.commandLine = commandLine
        self.timeout = timeout
        self.retry = retry
        self.delay = delay
        self.backoff = backoff

        self.validate()

        return self

    def setFromStrings(self, strings):
        commandLine, timeout, retry, backoff, delay = (strings.strip() + self.repeatedSeparator).split(self._separator)[0:5]
        return self.set(commandLine, int(timeout or "60"), int(retry or "3"), int(backoff or "0"), int(delay or "0"))

    def getParameterStringsFromJsonElement(self, jsonElement, key):
        return str(jsonElement[key]) if key in jsonElement else ""

    def setFromJsonElement(self, jsonElement):
        commandLine = self.getParameterStringsFromJsonElement(jsonElement, "commandLine")
        timeout = self.getParameterStringsFromJsonElement(jsonElement, "timeout")
        retry = self.getParameterStringsFromJsonElement(jsonElement, "retry")
        backoff = self.getParameterStringsFromJsonElement(jsonElement, "backoff")
        delay = self.getParameterStringsFromJsonElement(jsonElement, "delay")
        print(";".join([commandLine, timeout, retry, backoff, delay]))
        return CommandParameter().setFromStrings(";".join([commandLine, timeout, retry, backoff, delay]))

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

            for jsonKey, jsonElement in jsonCommandList.items():
                returnCode = self.__executeFromJsonElement(jsonKey, jsonElement)
                if self.__isDoStopAtError(returnCode):
                    print(f"Stop at {jsonKey}, return code: {returnCode}, threshold: {self._thresholdOfError}.")
                    break

            return self.tallyingAllResults()

    def __executeFromJsonElement(self, jsonKey, jsonElement):
        if not type(jsonElement) is list:
            returnCode = CommandLineExecutor().execute(CommandParameter().setFromJsonElement(jsonElement))
        else:
            commandList = []
            for element in jsonElement:
                commandList.append(CommandParameter().setFromJsonElement(element))

            returnCode = CommandListParallelExecutor().execute(commandList)
        self.collectResults(jsonKey, returnCode)

        return returnCode

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