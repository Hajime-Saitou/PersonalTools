import os
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import threading
import time

class FileWaiter(object):
    ticksPerSecond = 10

    def wait(self, filename, timeout=30):
        try:
            for _ in range(timeout * self.ticksPerSecond):
                if os.path.exists(filename):
                    break
                time.sleep(1 / self.ticksPerSecond)
            else:
                print(f"Timed out: File does not put at purpose directory. file:{filename}")
                return False
            
            return True
        except KeyboardInterrupt:
            print("Keyboard Interrupt: Wating loop was force end.")
            return False

class FileListWaiter(FileWaiter):
    @property
    def waitingFileCount(self):
        return self._waitingFileCount

    @property
    def existFileCount(self):
        return self._existFileCount

    def wait(self, filenameList, timeout=30):
        if not filenameList:
            raise ValueError("Waiting list is not set.")

        self._waitingFileCount = len(filenameList)
        self._existFileCount = 0

        # ThreadPoolExecuter.map need iteratable argument only.
        timeoutList = []
        for _ in range(self._waitingFileCount):
            timeoutList.append(timeout)

        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            allFutureResults = list(executor.map(super().wait, filenameList, timeoutList))
            executor.shutdown()

        for futureResults in list(allFutureResults):
            if futureResults == True:
                self._existFileCount += 1

        return (self._waitingFileCount - self._existFileCount) == 0

if __name__ == "__main__":
    waitingFileList = [
        os.path.join("c:\\temp", "foo.txt"),
        os.path.join("c:\\temp", "bar.txt"),
        os.path.join("c:\\temp", "poo.txt")
    ]

    results = FileWaiter().wait(waitingFileList[0], timeout=3)
    print(f"{results=}")

    flw = FileListWaiter()
    results = flw.wait(waitingFileList, timeout=3)
    print(f"{results=}, waiting: {flw.waitingFileCount}, exists: {flw.existFileCount}")

    exit(0)
