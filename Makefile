
src = src
out = output
input = /data/input
output = /data/output

YARN = $(HADOOP_HOME)/bin/yarn

app-compile:
	javac -classpath ${HADOOP_CLASSPATH} -d $(out)/app $(src)/App.java

app-package: app-compile
	jar -cvf App.jar -C $(out)/app .

app-prepare:
	hdfs dfs -test -d "${output}/app"; \
	if [ $$? -eq 0 ]; then \
		hdfs dfs -rm -r "${output}/app"; \
	fi

app-yarn: app-package app-prepare
	$(YARN) jar App.jar App $(input)/emp.txt $(output)/app

app-run: app-package app-prepare
	hadoop jar App.jar App $(input)/emp.txt $(output)/app


iot1-compile:
	javac -classpath $(HADOOP_CLASSPATH) -d $(out)/iot1 $(src)/IOT1.java

iot1-package: iot1-compile
	jar -cvf IOT1.jar -C $(out)/iot1 .

iot1-prepare:
	hdfs dfs -test -d "${output}/iot1"; \
	if [ $$? -eq 0 ] ; then \
		hdfs dfs -rm -r "${output}/iot1"; \
	fi

iot1-yarn: iot1-package iot1-prepare
	$(YARN) jar IOT1.jar IOT1 $(input)/device.txt $(input)/dvalues.txt $(output)/iot1

compile: app-compile iot1-compile

clean:
	rm -rf $(out)/* *.jar
