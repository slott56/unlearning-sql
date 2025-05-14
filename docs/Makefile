PLANTUML = java -jar ../plantuml-1.2024.6.jar

PNG_IMAGES = from_where_select.png
# TEX_IMAGES = from_where_select.tex
TIKZ_IMAGES = from_where_select.tikz

presentation.pdf : presentation.tex ${TIKZ_IMAGES}
	pdflatex -shell-escape presentation.tex
	pdflatex -shell-escape presentation.tex

%.tex : %.uml
	${PLANTUML} -progress -tlatex:nopreamble $<

%.tikz : %.uml
	${PLANTUML} -progress -tlatex:nopreamble $<
	mv $(basename $<).tex $@

%.png : %.uml
	${PLANTUML} -progress -tpng $<
