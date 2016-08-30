all:
	ocaml setup.ml -build && mv *.byte test/
oasis:
	oasis setup
	ocaml setup.ml -configure
clean:
	rm -rf _build setup.* myocamlbuild.ml _tags
	rm -rf test/*.byte
