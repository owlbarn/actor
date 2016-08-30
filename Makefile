all:
	ocaml setup.ml -build && mv *.byte test/
oasis:
	oasis setup
	ocaml setup.ml -configure
clean:
	rm -rf _build
	rm -rf test/*.byte test/*.native
cleanall:
	rm -rf _build setup.* myocamlbuild.ml _tags
	rm -rf test/*.byte test/*.native
