
src = src/App.java
out = output
application = App
input = /data/input
output = /data/output

compile:
	javac -classpath ${HADOOP_CLASSPATH} -d $(out) $(src)

package: compile
	jar -cvf App.jar -C $(out) .

run: package prepare
	hadoop jar App.jar $(application) $(input) $(output)

prepare:
	hdfs dfs -test -d $(output); \
	if [ $$? -eq 0 ]; then \
		hdfs dfs -rm -r $(output); \
	fi

yarn: package prepare
	yarn jar App.jar $(application) $(input) $(output)

clean:
	rm -rf $(out)/* App.jar
