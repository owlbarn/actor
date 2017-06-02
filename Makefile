all:
	ocaml setup.ml -build
oasis:
	oasis setup
	ocaml setup.ml -configure
install:
	ocaml setup.ml -uninstall
	ocaml setup.ml -install
uninstall:
	ocamlfind remove omap
	ocamlfind remove core
	ocamlfind remove optimise
clean:
	rm -rf _build
	rm -rf *.byte *.native
	rm -rf log/*
cleanall:
	rm -rf _build setup.* myocamlbuild.ml _tags
	rm -rf *.byte *.native
	rm -rf lib/META lib/*.mldylib lib/*.mllib
	rm -rf lib/core/META lib/core/*.mldylib lib/core/*.mllib
	rm -rf lib/optimise/META lib/optimise/*.mldylib lib/optimise/*.mllib
	rm -rf log/*
