from concurrent.futures import ThreadPoolExecutor
import subprocess
import multiprocessing

def executeCommandLine(commandParameter):
    commandLine, timeout, retry = commandParameter
    for trialCounter in range(retry):
        try:
            process = subprocess.Popen(commandLine, shell=True, text=True)
            process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            print(f"Timed out({trialCounter + 1}/{retry}): {commandLine}")
            process.terminate()
        else:
            return process.returncode

    return -1

def executeAllCommandLines(commandParameters):
    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        allFutureResults = list(executor.map(executeCommandLine, commandParameters))
        executor.shutdown()

    allResults = {0: 0}
    for futureResults in list(allFutureResults):
        incrementDict(allResults, futureResults)

    return allResults

def incrementDict(dictionary, key):
        if key not in dictionary:
            dictionary[key] = 1
        else:
            dictionary[key] = dictionary[key] + 1

def tallyingAllResults(returnCodeDict, numOfCommandLines):
    sortedByKeyAsc = sorted(returnCodeDict.items(), key=lambda item:item[0])
    print(f"All results: {sortedByKeyAsc}")

    if returnCodeDict[0] == numOfCommandLines:
        return 0

    returnCodeDict.pop(0)

    # If key value has non zero value , sort a dict by item value(this is counter).
    sortedByValueDesc = sorted(returnCodeDict.items(), key=lambda item:item[1], reverse=True)
    topOfCount = next(iter(sortedByValueDesc))[1]

    # If item value is not unique, pick up more highly key value.
    sortedByKeyDesc = sorted(returnCodeDict.items(), key=lambda item:item[0], reverse=True)
    for key, value in sortedByKeyDesc:
        if value == topOfCount:
            return key

commandLines = [
    ("echo hoge", 60, 5),
    ("timeout /t 2 /nobreak > nul", 1, 3)
]

returnCodeDict = executeAllCommandLines(commandLines)
finalResults = tallyingAllResults(returnCodeDict, len(commandLines))
print(finalResults)

exit(0)