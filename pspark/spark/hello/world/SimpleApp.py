__author__ = 'MK33'

# from pyspark import SparkContext
#
# logFile = "pom.xml"  # Should be some file on your system
# sc = SparkContext("local", "Simple App")
# logData = sc.textFile(logFile).cache()
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b:")