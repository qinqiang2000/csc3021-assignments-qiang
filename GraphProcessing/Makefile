JFLAGS = -g
JAVAC = javac

CLASS_STEMS = Relax SparseMatrix SparseMatrixCOO SparseMatrixCSR \
	      SparseMatrixCSC PageRank ConnectedComponents ParallelContext \
	      ParallelContextHolder ParallelContextSingleThread \
	      ParallelContextSimple DisjointSetCC

PKG_CLASS_FILES = $(patsubst %,uk/ac/qub/csc3021/graph/%.class,$(CLASS_STEMS))
PKG_JAVA_FILES = $(patsubst %,uk/ac/qub/csc3021/graph/%.java,$(CLASS_STEMS))

PACKAGE = lib/graph.jar

CLASS_FILES = DriverA3.class

SUBMIT_A2 = DriverA2.java $(PKG_JAVA_FILES)
SUBMIT_A3 = DriverA3.java $(PKG_JAVA_FILES)

ALL = $(PKG_CLASS_FILES) $(PACKAGE) $(CLASS_FILES)

all: $(ALL)

DriverA1.class: $(PKG_CLASS_FILES) $(PACKAGE) DriverA1.java
	$(JAVAC) $(JFLAGS) -cp . DriverA1.java

DriverA2.class: $(PKG_CLASS_FILES) $(PACKAGE) DriverA2.java
	$(JAVAC) $(JFLAGS) -cp . DriverA2.java

DriverA3.class: $(PKG_CLASS_FILES) $(PACKAGE) DriverA3.java
	$(JAVAC) $(JFLAGS) -cp . DriverA3.java

Validator.class: $(PKG_CLASS_FILES) $(PACKAGE) Validator.java
	$(JAVAC) $(JFLAGS) -cp . Validator.java

%.class: %.java
	$(JAVAC) -d . -cp . $(JFLAGS) $<

$(PACKAGE): $(PKG_CLASS_FILES)
	mkdir -p lib
	jar cf $(PACKAGE) $(PKG_CLASS_FILES)

submitA2.zip: $(SUBMIT_A2)
	zip submitA2.zip $(SUBMIT_A2)

submitA3.zip: $(SUBMIT_A3)
	zip submitA3.zip $(SUBMIT_A3)

clean:
	$(RM) $(ALL)
	$(RM) uk/ac/qub/csc3021/graph/*.class
