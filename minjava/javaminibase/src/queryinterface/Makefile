JDKPATH = ${JAVA_HOME}
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)


QueryInterface:QueryInterface.java
	$(JAVAC) QueryInterface.java

queryinterface: QueryInterface
	$(JAVA) queryinterface.QueryInterface
