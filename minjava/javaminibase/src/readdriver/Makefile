JDKPATH = ${JAVA_HOME}
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

ReadDriver:ReadDriver.java
	$(JAVAC) ReadDriver.java

readdriver: ReadDriver
	$(JAVA) readdriver.ReadDriver
