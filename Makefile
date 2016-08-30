all:
	ocaml setup.ml -build
oasis:
	oasis setup
	ocaml setup.ml -configure
clean:
	rm -rf _build
	rm -rf *.byte *.native
cleanall:
	rm -rf _build setup.* myocamlbuild.ml _tags
	rm -rf *.byte *.native
