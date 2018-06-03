
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

iot2-compile:
	javac -classpath $(HADOOP_CLASSPATH) -d $(out)/iot2 $(src)/IOT2.java

iot2-package: iot2-compile
	jar -cvf IOT2.jar -C $(out)/iot2 .

iot2-prepare:
	hdfs dfs -test -d "${output}/iot2"; \
	if [ $$? -eq 0 ] ; then \
		hdfs dfs -rm -r "${output}/iot2"; \
	fi

iot2-yarn: iot2-package iot2-prepare
	$(YARN) jar IOT2.jar IOT2 $(input)/device.txt $(input)/dvalues.txt $(output)/iot2

compile: app-compile iot1-compile iot2-compile

clean:
	rm -rf $(out)/* *.jar
