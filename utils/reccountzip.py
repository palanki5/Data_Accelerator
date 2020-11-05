import os
import sys
import gzip
import bz2
import zipfile
import logging


# Open zip file
def openzip(zfile):
    f = zipfile.ZipFile(zfile)
    firstFile = f.filelist[0].filename
    if firstFile.endswith("/"):
        raise Exception("This zip file contains a directory.")
    return f.open(firstFile)


# Open bz2 file
def openbz2(bz2file):
    return bz2.BZ2File(bz2file, "r")


# Class for compressedFile
class compressedFile:
    fileOpenerDict = {".gz": gzip.open, ".bz2": openbz2, ".zip": openzip}

    def __init__(self, pFilePath):
        logger = logging.getLogger(__name__)
        self.filePath = os.path.abspath(pFilePath)
        self.fileStem, self.ext = os.path.splitext(self.filePath)
        self.ext = self.ext.lower()
        try:
            self.fileOpener = self.fileOpenerDict[self.ext]
        except KeyError:
            logger.error("Only gz, zip, and bz2 files allowed.")
            sys.exit(1)

    def countLines(self):
        with self.fileOpener(self.filePath) as fh:
            count = 0
            for _ in fh:
                count += 1
            return count
