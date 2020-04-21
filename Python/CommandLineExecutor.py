from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing

class CommandParameter(object):
    commandLine = None
    timeout = None
    retry = None
    __hasSetThreshouldOfWarning = False
    __hasSetThreshouldOfError = False

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
    def read(self, filename):
        commandParameters = []

        with open(filename) as f:
            for lineNo, readLine in enumerate(f.readlines()):
                readLine = readLine.strip()
                if not readLine:
                    print(f"Line {lineNo + 1}: Command line is blank.")
                    continue

                commandLine, timeout, retry = readLine.split(";")
                commandParameters.append(CommandParameter(commandLine, int(timeout), int(retry)))
        return commandParameters

class CommandListExecutor(CommandLineExecutor):
    _numOfcommandParameters = 0
    __allResults = {0: 0}

    def _incrementAllResults(self,  key):
            if key not in self.__allResults:
                self.__allResults[key] = 1
            else:
                self.__allResults[key] = self.__allResults[key] + 1

    def _tallyingAllResults(self):
        sortedByKeyAsc = sorted(self.__allResults.items(), key=lambda item:item[0])
        print(f"All results: {sortedByKeyAsc}")

        if self.__allResults[0] == self._numOfcommandParameters:
            return 0

        del self.__allResults[0]

        # If key value has non zero value , sort a dict by item value(this is counter).
        sortedByValueDesc = sorted(self.__allResults.items(), key=lambda item:item[1], reverse=True)
        topOfCount = next(iter(sortedByValueDesc))[1]

        # If item value is not unique, pick up more highly key value.
        sortedByKeyDesc = sorted(self.__allResults.items(), key=lambda item:item[0], reverse=True)
        for key, value in sortedByKeyDesc:
            if value == topOfCount:
                return key

class CommandListParallelExecutor(CommandListExecutor):
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

class CommandListSerialExecutor(CommandListExecutor):
    __hasSetThreshouldOfError = False

    @property
    def thresholdOfError(self, value):
        return self.__thresholdOfError

    @thresholdOfError.setter
    def thresholdOfError(self, value):
        if value < 0:
            raise ValueError("Set threshould to greater than or equals 0.")

        self.__hasSetThreshouldOfError = True
        self.__thresholdOfError = value

    def execute(self, commandParameters):
        if not commandParameters:
            raise ValueError("Command parameters not set.")

        self._numOfcommandParameters = len(commandParameters)

        for index, parameter in enumerate(commandParameters):
            returnCode = super().execute(parameter)
            self._incrementAllResults(returnCode)
            if self.__isDoStopAtError(returnCode):
                print(f"Stop at {index + 1}: return code is {returnCode}, threshould is {self.__thresholdOfError}.")
                break

        return self._tallyingAllResults()

    def __isDoStopAtError(self, returnCode):
        if self.__hasSetThreshouldOfError == False:
            return False
        
        return True if returnCode >= self.__thresholdOfError else False

if __name__ == "__main__":
    parameter = [
        CommandParameter("echo hoge", 10, 3),
        CommandParameter("timeout /t 3 /nobreak > nul", 1, 5)
    ]
    CommandListParallelExecutor().execute(parameter)

    exit(0)