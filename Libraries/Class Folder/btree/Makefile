JAR=heapAssign.jar
ASSIGNMENT=HeapFile
ASSIGN=/p/course/cs784-raghu/minibase_share/javaminibase/javamini_hwk/assign

JDKPATH = /s/java
LIBPATH = $(ASSIGN)/$(ASSIGNMENT)/lib/$(JAR)
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core 
