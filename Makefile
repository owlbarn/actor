.PHONY: all
all: build

.PHONY: depend depends
depend depends:
	jbuilder external-lib-deps --missing @install @runtest

.PHONY: build
build: depends
	jbuilder build @install

.PHONY: test
test: depends
	jbuilder runtest -j 1 --no-buffer -p owl

.PHONY: clean
clean:
	jbuilder clean

.PHONY: install
install: build
	dune install

.PHONY: uninstall
uninstall:
	dune uninstall

.PHONY: doc
doc:
	dune build @doc

.PHONY: cleanall
cleanall:
	dune uninstall && dune clean
	$(RM) -r $(find . -name .merlin)
