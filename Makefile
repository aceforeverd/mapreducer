
src = src/App.java
out = output
application = App
input = /data/inupt
output = /data/output

compile:
	javac -classpath ${HADOOP_CLASSPATH} -d $(out) $(src)

package: compile
	jar -cvf App.jar -C $(out) .

run: package
	hadoop jar App.jar $(application) $(input) $(output)

yarn: package
	yrn App.jar $(application) "$(input)/*" $(output)

clean:
	rm -rf $(out)/* App.jar
