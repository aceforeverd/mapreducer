
src = src
out = output
input = /data/input
output = /data/output

YARN = $(HADOOP_HOME)/bin/yarn

app-prepare:
	if [ ! -d $(out)/app ]; then \
		mkdir -p $(out)/app; \
	fi
	hdfs dfs -test -d "${output}/app"; \
	if [ $$? -eq 0 ]; then \
		hdfs dfs -rm -r "${output}/app"; \
	fi

app-compile: app-prepare
	javac -classpath ${HADOOP_CLASSPATH} -d $(out)/app $(src)/App.java

app-package: app-compile
	jar -cvf App.jar -C $(out)/app .

app-yarn: app-package 
	$(YARN) jar App.jar App $(input)/emp.txt $(output)/app

app-run: app-package app-prepare
	hadoop jar App.jar App $(input)/emp.txt $(output)/app


iot1-prepare:
	if [ ! -d $(out)/iot1 ]; then \
		mkdir -p $(out)/iot1; \
	fi
	hdfs dfs -test -d "${output}/iot1"; \
	if [ $$? -eq 0 ] ; then \
		hdfs dfs -rm -r "${output}/iot1"; \
	fi

iot1-compile: iot1-prepare
	javac -classpath $(HADOOP_CLASSPATH) -d $(out)/iot1 $(src)/IOT1.java

iot1-package: iot1-compile
	jar -cvf IOT1.jar -C $(out)/iot1 .

iot1-yarn: iot1-package iot1-prepare
	$(YARN) jar IOT1.jar IOT1 $(input)/device.txt $(input)/dvalues.txt $(output)/iot1



iot2-prepare:
	if [ ! -d $(out)/iot2 ]; then \
		mkdir -p $(out)/iot2; \
	fi
	hdfs dfs -test -d "${output}/iot2"; \
	if [ $$? -eq 0 ] ; then \
		hdfs dfs -rm -r "${output}/iot2"; \
	fi

iot2-compile: iot2-prepare
	javac -classpath $(HADOOP_CLASSPATH) -d $(out)/iot2 $(src)/IOT2.java

iot2-package: iot2-compile
	jar -cvf IOT2.jar -C $(out)/iot2 .


iot2-yarn: iot2-package
	$(YARN) jar IOT2.jar IOT2 $(input)/device.txt $(input)/dvalues.txt $(output)/iot2

clean:
	rm -rf $(out)/* *.jar
