import os
import time
import datetime
import pandas as pd
import pyarrow as pa
from struct import unpack
import pyarrow.parquet as pq
from datetime import datetime as dttime
import zipfile

# environment variables
os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-8-openjdk-amd64")
os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "/opt/hadoop/")
os.environ["CLASSPATH"] = os.getenv("CLASSPATH", "/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/httpclient-4.2.5.jar:/opt/hadoop/share/hadoop/common/lib/commons-math3-3.1.1.jar:/opt/hadoop/share/hadoop/common/lib/junit-4.11.jar:/opt/hadoop/share/hadoop/common/lib/stax-api-1.0-2.jar:/opt/hadoop/share/hadoop/common/lib/gson-2.2.4.jar:/opt/hadoop/share/hadoop/common/lib/guava-11.0.2.jar:/opt/hadoop/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/commons-digester-1.8.jar:/opt/hadoop/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop/share/hadoop/common/lib/zookeeper-3.4.6.jar:/opt/hadoop/share/hadoop/common/lib/jersey-core-1.9.jar:/opt/hadoop/share/hadoop/common/lib/curator-recipes-2.7.1.jar:/opt/hadoop/share/hadoop/common/lib/jaxb-api-2.2.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-httpclient-3.1.jar:/opt/hadoop/share/hadoop/common/lib/paranamer-2.3.jar:/opt/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/common/lib/avro-1.7.4.jar:/opt/hadoop/share/hadoop/common/lib/commons-configuration-1.6.jar:/opt/hadoop/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar:/opt/hadoop/share/hadoop/common/lib/jettison-1.1.jar:/opt/hadoop/share/hadoop/common/lib/asm-3.2.jar:/opt/hadoop/share/hadoop/common/lib/netty-3.6.2.Final.jar:/opt/hadoop/share/hadoop/common/lib/servlet-api-2.5.jar:/opt/hadoop/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/jets3t-0.9.0.jar:/opt/hadoop/share/hadoop/common/lib/jersey-server-1.9.jar:/opt/hadoop/share/hadoop/common/lib/jetty-sslengine-6.1.26.jar:/opt/hadoop/share/hadoop/common/lib/httpcore-4.2.5.jar:/opt/hadoop/share/hadoop/common/lib/hadoop-auth-2.7.5.jar:/opt/hadoop/share/hadoop/common/lib/jsp-api-2.1.jar:/opt/hadoop/share/hadoop/common/lib/htrace-core-3.1.0-incubating.jar:/opt/hadoop/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/common/lib/curator-client-2.7.1.jar:/opt/hadoop/share/hadoop/common/lib/hamcrest-core-1.3.jar:/opt/hadoop/share/hadoop/common/lib/mockito-all-1.8.5.jar:/opt/hadoop/share/hadoop/common/lib/xmlenc-0.52.jar:/opt/hadoop/share/hadoop/common/lib/commons-beanutils-1.7.0.jar:/opt/hadoop/share/hadoop/common/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/opt/hadoop/share/hadoop/common/lib/jetty-util-6.1.26.jar:/opt/hadoop/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/commons-io-2.4.jar:/opt/hadoop/share/hadoop/common/lib/hadoop-annotations-2.7.5.jar:/opt/hadoop/share/hadoop/common/lib/snappy-java-1.0.4.1.jar:/opt/hadoop/share/hadoop/common/lib/api-util-1.0.0-M20.jar:/opt/hadoop/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/commons-compress-1.4.1.jar:/opt/hadoop/share/hadoop/common/lib/commons-net-3.1.jar:/opt/hadoop/share/hadoop/common/lib/slf4j-api-1.7.10.jar:/opt/hadoop/share/hadoop/common/lib/activation-1.1.jar:/opt/hadoop/share/hadoop/common/lib/api-asn1-api-1.0.0-M20.jar:/opt/hadoop/share/hadoop/common/lib/commons-collections-3.2.2.jar:/opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar:/opt/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/opt/hadoop/share/hadoop/common/lib/commons-codec-1.4.jar:/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:/opt/hadoop/share/hadoop/common/lib/apacheds-i18n-2.0.0-M15.jar:/opt/hadoop/share/hadoop/common/lib/jersey-json-1.9.jar:/opt/hadoop/share/hadoop/common/lib/java-xmlbuilder-0.4.jar:/opt/hadoop/share/hadoop/common/lib/jsch-0.1.54.jar:/opt/hadoop/share/hadoop/common/lib/jsr305-3.0.0.jar:/opt/hadoop/share/hadoop/common/lib/curator-framework-2.7.1.jar:/opt/hadoop/share/hadoop/common/lib/xz-1.0.jar:/opt/hadoop/share/hadoop/common/lib/jetty-6.1.26.jar:/opt/hadoop/share/hadoop/common/hadoop-common-2.7.5.jar:/opt/hadoop/share/hadoop/common/hadoop-common-2.7.5-tests.jar:/opt/hadoop/share/hadoop/common/hadoop-nfs-2.7.5.jar:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/guava-11.0.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-core-1.9.jar:/opt/hadoop/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/xercesImpl-2.9.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/asm-3.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/netty-3.6.2.Final.jar:/opt/hadoop/share/hadoop/hdfs/lib/servlet-api-2.5.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-server-1.9.jar:/opt/hadoop/share/hadoop/hdfs/lib/htrace-core-3.1.0-incubating.jar:/opt/hadoop/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/xmlenc-0.52.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-util-6.1.26.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-io-2.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/xml-apis-1.3.04.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-codec-1.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/opt/hadoop/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/opt/hadoop/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-6.1.26.jar:/opt/hadoop/share/hadoop/hdfs/lib/netty-all-4.0.23.Final.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.7.5-tests.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.7.5.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/lib/guice-3.0.jar:/opt/hadoop/share/hadoop/yarn/lib/stax-api-1.0-2.jar:/opt/hadoop/share/hadoop/yarn/lib/guava-11.0.2.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/yarn/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop/share/hadoop/yarn/lib/zookeeper-3.4.6.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-core-1.9.jar:/opt/hadoop/share/hadoop/yarn/lib/jaxb-api-2.2.2.jar:/opt/hadoop/share/hadoop/yarn/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/yarn/lib/zookeeper-3.4.6-tests.jar:/opt/hadoop/share/hadoop/yarn/lib/jettison-1.1.jar:/opt/hadoop/share/hadoop/yarn/lib/asm-3.2.jar:/opt/hadoop/share/hadoop/yarn/lib/netty-3.6.2.Final.jar:/opt/hadoop/share/hadoop/yarn/lib/servlet-api-2.5.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-xc-1.9.13.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-server-1.9.jar:/opt/hadoop/share/hadoop/yarn/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/yarn/lib/jetty-util-6.1.26.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-io-2.4.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-compress-1.4.1.jar:/opt/hadoop/share/hadoop/yarn/lib/guice-servlet-3.0.jar:/opt/hadoop/share/hadoop/yarn/lib/activation-1.1.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-collections-3.2.2.jar:/opt/hadoop/share/hadoop/yarn/lib/javax.inject-1.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-logging-1.1.3.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-client-1.9.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-codec-1.4.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-cli-1.2.jar:/opt/hadoop/share/hadoop/yarn/lib/aopalliance-1.0.jar:/opt/hadoop/share/hadoop/yarn/lib/commons-lang-2.6.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-json-1.9.jar:/opt/hadoop/share/hadoop/yarn/lib/leveldbjni-all-1.8.jar:/opt/hadoop/share/hadoop/yarn/lib/jsr305-3.0.0.jar:/opt/hadoop/share/hadoop/yarn/lib/xz-1.0.jar:/opt/hadoop/share/hadoop/yarn/lib/jetty-6.1.26.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-guice-1.9.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-nodemanager-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-api-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-client-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-tests-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-registry-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-web-proxy-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-common-2.7.5.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-common-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/lib/junit-4.11.jar:/opt/hadoop/share/hadoop/mapreduce/lib/guice-3.0.jar:/opt/hadoop/share/hadoop/mapreduce/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/mapreduce/lib/jersey-core-1.9.jar:/opt/hadoop/share/hadoop/mapreduce/lib/paranamer-2.3.jar:/opt/hadoop/share/hadoop/mapreduce/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/mapreduce/lib/avro-1.7.4.jar:/opt/hadoop/share/hadoop/mapreduce/lib/asm-3.2.jar:/opt/hadoop/share/hadoop/mapreduce/lib/netty-3.6.2.Final.jar:/opt/hadoop/share/hadoop/mapreduce/lib/jersey-server-1.9.jar:/opt/hadoop/share/hadoop/mapreduce/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/opt/hadoop/share/hadoop/mapreduce/lib/commons-io-2.4.jar:/opt/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/lib/snappy-java-1.0.4.1.jar:/opt/hadoop/share/hadoop/mapreduce/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/mapreduce/lib/commons-compress-1.4.1.jar:/opt/hadoop/share/hadoop/mapreduce/lib/guice-servlet-3.0.jar:/opt/hadoop/share/hadoop/mapreduce/lib/javax.inject-1.jar:/opt/hadoop/share/hadoop/mapreduce/lib/aopalliance-1.0.jar:/opt/hadoop/share/hadoop/mapreduce/lib/leveldbjni-all-1.8.jar:/opt/hadoop/share/hadoop/mapreduce/lib/xz-1.0.jar:/opt/hadoop/share/hadoop/mapreduce/lib/jersey-guice-1.9.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-2.7.5.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.5-tests.jar:/opt/hadoop//contrib/capacity-scheduler/*.jar")
os.environ["ARROW_LIBHDFS_DIR"] = os.getenv("ARROW_LIBHDFS_DIR", "/opt/hadoop/lib/native/")

#nbSensors
nbSensors = 22

# hdfs parameters
hdfsStart = 'hdfs://'
hdfsHost = 'artha.node1.pro.hupi.loc'
hdfsPort = '8020'
hdfsUser = 'ddamico'

# path of the file we want to read
inputTodoPath = '/user/' + hdfsUser + '/RAW_FILES/TO_DO/'
# path where we copy the file that has been read
inputDonePath = '/user/' + hdfsUser + '/RAW_FILES/DONE/'
# path of the file we write
outputPath = '/user/' + hdfsUser + '/DECIMAL_MICROAMPERE_DATA/UNFILTERED/TO_DO/'

# error path
errorPath = '/user/' + hdfsUser + '/RAW_FILES/ERROR/'
# error file
errorFile = 'error.txt'

# get the file system
fs = pa.hdfs.connect(hdfsHost, int(hdfsPort), user = hdfsUser)

# get the file list
fileList = fs.ls(hdfsStart + hdfsHost + ':' + hdfsPort + inputTodoPath)

# loop on each file of the list
for fullFileName in fileList:
	# get the name of the file
	fileName = fullFileName.split('/')[-1]
	print('file start : ' + fileName)
	# get file path
	path = hdfsStart + hdfsHost + ':' + hdfsPort + inputTodoPath + fileName
	with fs.open(path, 'rb') as f:
		try:
			# get the content in the zip
			archive = zipfile.ZipFile(f, 'r')
			# get the first file ine the list of files in the zip
			rawFileName = archive.namelist()[0]
			# get the data in the raw file
			binaryContent = archive.read(rawFileName)
				
			# convert the data
			shortArray = unpack('<' + str(len(binaryContent)//2) + 'h', binaryContent)
			
			# create the array of data for each sensor
			# length of the data
			shortArraySize = len(shortArray)
			# Array of array representing each sensor
			sensors = [[] for i in range(nbSensors)]
			# index of the first 22
			value22Id = shortArray.index(nbSensors)
			while(value22Id < shortArraySize):
				# number of values in a row for each sensor
				nbValues = shortArray[value22Id + 2]
				# first value of sensors
				valuesStartId = value22Id + 4
				# loop to get all values for each sensor
				for i in range(0, nbSensors):
					valuesEndId = valuesStartId + nbValues
					sensors[i].extend(shortArray[valuesStartId : valuesEndId])
					valuesStartId = valuesEndId
				value22Id = valuesStartId

			# get file extension
			inputFileExtension = '.' + rawFileName.split('.')[-1]
			# get the date from the file name
			year = rawFileName[0:4]
			month = rawFileName[4:6]
			day = rawFileName[6:8]
			hours = rawFileName[9:11]
			minutes = rawFileName[11:13]
			seconds = rawFileName[13:15]
			milliseconds = '000000'
			# Concatenate the date with the good format
			dateString = day + '.' + month + '.' + year + ' ' + hours + ':' + minutes + ':' + seconds + ',' + milliseconds
			# Convert the string to date
			dateDate = dttime.strptime(dateString, '%d.%m.%Y %H:%M:%S,%f')
			
			# Array of time stamps
			timeStamps = []
			for i in range(0, len(sensors[0])):
				timeStamp = dateDate.strftime('%s.%f')
				timeStamps.append(timeStamp)
				dateDate = dateDate + datetime.timedelta(microseconds=100)

			# Convert sensors data to DF
			df = pd.DataFrame({0: sensors[0], 1: sensors[1], 2: sensors[2], 3: sensors[3], 4: sensors[4], 5: sensors[5], 6: sensors[6], 7: sensors[7], 8: sensors[8], 9: sensors[9], 10: sensors[10], 11: sensors[11], 12: sensors[12], 13: sensors[13], 14: sensors[14], 15: sensors[15], 16: sensors[16], 17: sensors[17], 18: sensors[18], 19: sensors[19], 20: sensors[20], 21: sensors[21]}, index=timeStamps)
			
			table = pa.Table.from_pandas(df)
			
			outputDirectory = rawFileName.replace(inputFileExtension, '')
			pq.write_to_dataset(table, root_path = hdfsStart + hdfsHost + ':' + hdfsPort + outputPath + outputDirectory, filesystem = fs)
			
			# use rename function to move the file
			fs.rename(inputTodoPath + fileName, inputDonePath + fileName)
			
			print('file done : ' + fileName)
		except:
			# use rename function to move the file
			fs.rename(inputTodoPath + fileName, errorPath + fileName)
			# write error
			path = hdfsStart + hdfsHost + ':' + hdfsPort + errorPath + errorFile
			with fs.open(path, 'ab') as f:
				f.write('Error in file ' + fullFileName)