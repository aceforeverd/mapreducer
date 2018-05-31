
src = src/App.java
out = output
application = App
input = /data/inupt
output = /data/output

compile:
	javac -classpath $HADOOP_CLASSPATH -d $(out) $(src)

package: compile
	jar -cvf App.jar -C $(out)

run: package
	hadoop jar App.jar $(application) $(input) $(output)

yarn: package
	yarn App.jar $(application) "$(input)/*" $(output)
